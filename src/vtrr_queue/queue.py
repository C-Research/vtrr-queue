import json
import logging
import uuid
from pathlib import Path
from typing import Any, Callable
from celery import Task


import redis as redis_lib

logger = logging.getLogger(__name__)

_SCRIPTS_DIR = Path(__file__).parent / "scripts"

_QUEUE_KEY = "vtrr:queue"
_CURRENT_VT_KEY = "vtrr:current_virtual_time"
_TASK_LOOKUPS_KEY = "vtrr:task"
_USERS_VT_KEY = "vtrr:user_virtual_time"

_ENQUEUE_KEYS = [_CURRENT_VT_KEY, _USERS_VT_KEY, _QUEUE_KEY, _TASK_LOOKUPS_KEY]
_DEQUEUE_KEYS = [_QUEUE_KEY, _CURRENT_VT_KEY, _TASK_LOOKUPS_KEY, _USERS_VT_KEY]


class VTRRTask:
    def __init__(self, fn: Callable, vtrr: "VTRRQueue", celery_task: Any) -> None:
        self._fn = fn
        self._vtrr = vtrr
        self._celery_task = celery_task
        self.__name__ = fn.__name__
        self.__module__ = fn.__module__
        self.__doc__ = fn.__doc__

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._fn(*args, **kwargs)

    @property
    def name(self) -> str:
        return f"{self.__module__}.{self.__name__}"

    def queue(self, user_id: str, tasks: list[dict[str, Any]]) -> None:
        """
        Bulk-enqueue tasks into the VTRR queue.

        Each entry in `tasks` should contain "args" (list) and/or "kwargs" (dict).
        Celery dequeue workers are scheduled automatically after enqueuing.

        Example:
            process_file.queue(
                user_id="u_123",
                tasks=[
                    {"args": ["s3://bucket/a.pdf"], "kwargs": {"force_ocr": True}},
                    {"args": ["s3://bucket/b.pdf"]},
                ],
            )
        """
        if not user_id:
            raise ValueError("user_id is required")
        if not tasks:
            return

        argv: list[str] = []
        for task in tasks:
            task_id = str(task.get("id", uuid.uuid4()))
            payload = json.dumps(
                {
                    "task_name": self.name,
                    "args": task.get("args", []),
                    "kwargs": task.get("kwargs", {}),
                }
            )
            argv += [user_id, task_id, payload]

        self._vtrr._enqueue(argv)
        # TODO: Make the enqueue not commit until schedule_workers succeeds
        self._vtrr._schedule_workers(len(tasks), self._celery_task)


class _VTRRCeleryTask(Task):
    """Base task for VTRR-dispatched functions.

    Overriding retry() means a task can only ever re-run *itself* with its
    own payload
    """

    def retry(self, args=None, kwargs=None, exc=None, **options):
        payload = getattr(self.request, "vtrr_payload", None)
        return super().retry(
            args=(),
            kwargs={"task_payload": payload},
            exc=exc,
            **options,
        )


def _resolve_base(user_base: type | None) -> type:
    """Weave _VTRRTask into the user's custom Task base.

    _VTRRTask goes first in the MRO so its retry() normalizes the call into
    the current payload before any user-defined retry runs.
    """
    if user_base is None or user_base is Task:
        return _VTRRCeleryTask
    if issubclass(user_base, _VTRRCeleryTask):
        return user_base
    return type(f"VTRR{user_base.__name__}", (_VTRRCeleryTask, user_base), {})


class VTRRQueue:
    DEFAULT_MAX_CONCURRENCY = 4

    def __init__(
        self,
        redis_client: redis_lib.Redis,
        celery_app: Any,
        *,
        celery_queue: str = "vtrr_queue",
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    ) -> None:
        self._redis = redis_client
        self._celery_app = celery_app
        self._celery_queue = celery_queue
        self._max_concurrency = max_concurrency
        self._registry: dict[str, VTRRTask] = {}
        self._enqueue_script = self._load_script("enqueue")
        self._dequeue_script = self._load_script("dequeue")
        self._broker: redis_lib.Redis | None = self._connect_broker()

    def task(self, fn: Callable | None = None, **task_options: Any) -> Any:
        """
        Decorator that registers a function as a VTRR-dispatchable Celery task.

        The first argument is always `self` (the bound Celery task), then any args/kwargs from .queue():

            @vtrr.task(autoretry_for=(TransientError,), max_retries=3,
                       retry_backoff=True)
            def my_task(self, task_id, x, y): ...
        """

        def decorator(f: Callable) -> VTRRTask:
            vtrr = self
            options = dict(task_options)
            options.pop("bind", None)  # always bound
            base = _resolve_base(options.pop("base", None))
            task_name = f"{f.__module__}.{f.__name__}"

            @self._celery_app.task(name=task_name, bind=True, base=base, **options)
            def celery_wrapper(
                celery_task: Any, task_payload: dict | None = None
            ) -> None:
                dispatched = task_payload is None

                if dispatched:
                    result = vtrr._dequeue()
                    if result is None:
                        # TODO: schedule 1 retry if it's an original task
                        return  # queue empty — stop draining, no reschedule
                    dequeued_name, args, kwargs = result
                    task_payload = {
                        "task_name": dequeued_name,
                        "args": args,
                        "kwargs": kwargs,
                    }
                else:
                    dequeued_name = task_payload["task_name"]
                    args = task_payload["args"]
                    kwargs = task_payload["kwargs"]

                # Per-invocation stash (safe under threaded/gevent pools);
                # retry() reads this to re-run the same item.
                celery_task.request.vtrr_payload = task_payload

                try:
                    registered = vtrr._registry.get(dequeued_name)
                    if registered is None:
                        logger.error(
                            "vtrr_queue: unknown task %r — dropped", dequeued_name
                        )
                        return
                    registered._fn(celery_task, *args, **kwargs)
                finally:
                    if dispatched:
                        # In case there's an exception, schedule next dequeue
                        vtrr._schedule_next(celery_task)

            wrapped = VTRRTask(f, self, celery_wrapper)
            self._registry[wrapped.name] = wrapped
            return wrapped

        if fn is not None:
            return decorator(fn)
        return decorator

    # ------------------------------------------------------------------
    # Internal: setup
    # ------------------------------------------------------------------

    def _load_script(self, name: str) -> Any:
        src = (_SCRIPTS_DIR / f"{name}.lua").read_text()
        return self._redis.register_script(src)

    def _connect_broker(self) -> redis_lib.Redis | None:
        try:
            return redis_lib.from_url(self._celery_app.conf.broker_url)
        except Exception as err:
            logger.warning("vtrr_queue: could not connect to Celery broker: %s", err)
            return None

    # ------------------------------------------------------------------
    # Internal: Redis operations
    # ------------------------------------------------------------------

    def _enqueue(self, argv: list[str]) -> None:
        self._enqueue_script(keys=_ENQUEUE_KEYS, args=argv)

    def _dequeue(self) -> tuple[str, str, list, dict] | None:
        raw = self._dequeue_script(keys=_DEQUEUE_KEYS, args=[])
        if not raw:
            return None
        _, payload_bytes = raw
        if isinstance(payload_bytes, (bytes, bytearray)):
            payload_bytes = payload_bytes.decode()
        payload = json.loads(payload_bytes)
        return (
            payload["task_name"],
            payload["args"],
            payload["kwargs"],
        )

    # ------------------------------------------------------------------
    # Internal: worker scheduling
    # ------------------------------------------------------------------

    def _get_workers_scheduled(self) -> int:
        """
        Count pending messages in the Celery broker queue.
        Does NOT include workers already running; returns 0 on any error
        so the caller may over-schedule slightly, which is safe.
        """
        if self._broker is None:
            return 0
        try:
            q = self._celery_queue
            # Celery-managed Redis priority suffixes: 0 = no suffix, 3/6/9 = :<p>
            return sum(
                self._broker.llen(q if p == 0 else f"{q}:{p}") for p in (0, 3, 6, 9)
            )
        except Exception as err:
            logger.error(
                "vtrr_queue: could not check broker queue length: %s",
                err,
                exc_info=True,
            )
            return 0

    def _schedule_workers(self, num_tasks: int, dispatch_task: Any) -> None:
        workers_scheduled = self._get_workers_scheduled()
        to_schedule = min(self._max_concurrency - workers_scheduled, num_tasks)
        for _ in range(to_schedule):
            dispatch_task.apply_async(queue=self._celery_queue)

    def _schedule_next(self, current_celery_task: Any) -> None:
        """Schedule one more dequeue worker if tasks remain in the queue."""
        try:
            if self._redis.zcard(_QUEUE_KEY) > 0:
                current_celery_task.apply_async(queue=self._celery_queue)
        except Exception as err:
            logger.error(
                "vtrr_queue: could not schedule next dequeue worker: %s",
                err,
                exc_info=True,
            )

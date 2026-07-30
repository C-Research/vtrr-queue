import json
import logging
import uuid
from pathlib import Path
from typing import Any, Callable

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
        self._celery_task = celery_task  # the @celery_app.task-wrapped version of fn
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
                    "task_id": task_id,
                    "args": task.get("args", []),
                    "kwargs": task.get("kwargs", {}),
                }
            )
            argv += [user_id, task_id, payload]

        self._vtrr._enqueue(argv)
        self._vtrr._schedule_workers(len(tasks))


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
        self._register_dispatcher()

    def task(self, fn: Callable | None = None, **task_options: Any) -> Any:
        """
        Decorator that registers a function as a VTRR-dispatchable Celery task.

        Accepts the same keyword arguments as @celery_app.task():

            @vtrr.task(base=MyBaseTask, max_retries=3, retry_kwargs={"countdown": 10})
            def my_task(task_id, x, y): ...
        """

        def decorator(f: Callable) -> VTRRTask:
            celery_task = self._celery_app.task(
                name=f"{f.__module__}.{f.__name__}",
                **task_options,
            )(f)
            wrapped = VTRRTask(f, self, celery_task)
            self._registry[wrapped.name] = wrapped
            return wrapped

        if fn is not None:
            # Used as @vtrr.task with no arguments
            return decorator(fn)
        # Used as @vtrr.task(...) with arguments
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

    def _register_dispatcher(self) -> None:
        """
        Register the internal dequeue-and-dispatch task on the user's Celery app.
        This task pops one item from Redis and forwards it to the registered Celery task.
        """
        vtrr = self

        @self._celery_app.task(name="vtrr_queue.dequeue_and_dispatch")
        def dequeue_and_dispatch() -> None:
            result = vtrr._dequeue()
            if result is None:
                return
            task_name, task_id, args, kwargs = result
            registered = vtrr._registry.get(task_name)
            if registered is None:
                logger.error("vtrr_queue: unknown task %r — dropped", task_name)
                return
            registered._celery_task.apply_async(
                args=[task_id, *args],
                kwargs=kwargs,
                queue=vtrr._celery_queue,
            )

        self._dispatch_task = dequeue_and_dispatch

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
            payload["task_id"],
            payload["args"],
            payload["kwargs"],
        )

    # ------------------------------------------------------------------
    # Internal: worker scheduling
    # ------------------------------------------------------------------

    def _get_workers_scheduled(self) -> int:
        """
        Count queued dequeue_and_dispatch messages in the Celery broker.
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

    def _schedule_workers(self, num_tasks: int) -> None:
        workers_scheduled = self._get_workers_scheduled()
        to_schedule = min(self._max_concurrency - workers_scheduled, num_tasks)
        for _ in range(to_schedule):
            self._dispatch_task.apply_async(queue=self._celery_queue)

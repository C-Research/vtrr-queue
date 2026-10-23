import json
import logging
import uuid
from pathlib import Path
from typing import Any, Callable
from celery import Task
from celery.exceptions import Retry


import redis as redis_lib

logger = logging.getLogger(__name__)

_SCRIPTS_DIR = Path(__file__).parent / "scripts"


def _decode(value: bytes | str) -> str:
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


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

    def queue(self, partition_key: str, tasks: list[dict[str, Any]]) -> None:
        """
        Bulk-enqueue tasks into the VTRR queue.

        Each entry in `tasks` should contain "weight" (int) "args" (list) and/or "kwargs" (dict).
        Celery dequeue workers are scheduled automatically after enqueuing.

        Example:
            process_file.queue(
                partition_key="u_123",
                tasks=[
                    {"weight": 4, "args": ["s3://bucket/a.pdf"], "kwargs": {"force_ocr": True}},
                    {"weight": 10, "args": ["s3://bucket/b.pdf"]},
                ],
            )
        """
        if not partition_key:
            raise ValueError("key_id is required")
        if not tasks:
            return

        argv: list[str] = []
        for task in tasks:
            task_id = str(task.get("id", uuid.uuid4()))
            task_weight = int(task.get("weight", 1))
            payload = json.dumps(
                {
                    "task_name": self.name,
                    "args": task.get("args", []),
                    "kwargs": task.get("kwargs", {}),
                }
            )
            argv += [partition_key, task_id, task_weight, payload]

        self._vtrr._schedule_workers(len(tasks), self._celery_task)
        self._vtrr._enqueue(argv)


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
    DEFAULT_LEASE_SECONDS = 3900

    def __init__(
        self,
        redis_client: redis_lib.Redis,
        celery_app: Any,
        *,
        name: str,
        celery_queue: str = "vtrr_queue",
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        reclaim_interval: float | None = None,
        reclaim_queue: str | None = None,
    ) -> None:
        if reclaim_interval is not None and reclaim_interval <= 0:
            raise ValueError("reclaim_interval must be a positive number of seconds")
        self._redis = redis_client
        self._celery_app = celery_app
        self._celery_queue = celery_queue
        self._max_concurrency = max_concurrency
        self._lease_seconds = lease_seconds
        self._reclaim_interval = reclaim_interval
        self._reclaim_queue = reclaim_queue
        self._reclaim_task_name = f"vtrr_queue.reclaim.{name}"
        self._name = name
        self._task: VTRRTask | None = None

        prefix = f"vtrr:{name}"
        self._queue_key = f"{prefix}:queue"
        self._current_vt_key = f"{prefix}:current_virtual_time"
        self._task_key = f"{prefix}:task"
        self._partitions_vt_key = f"{prefix}:partition_virtual_time"
        self._meta_key = f"{prefix}:meta"
        self._inflight_key = f"{prefix}:inflight"
        self._inflight_task_key = f"{prefix}:inflight_task"
        self._lease_key = f"{prefix}:lease"
        self._timeouts_key = f"{prefix}:timeouts"
        # Every Lua script takes the same KEYS (see scripts/common.lua)
        self._keys = [
            self._queue_key,
            self._current_vt_key,
            self._partitions_vt_key,
            self._task_key,
            self._meta_key,
            self._inflight_key,
            self._inflight_task_key,
            self._lease_key,
            self._timeouts_key,
        ]
        self._enqueue_keys = self._dequeue_keys = self._keys

        self._common_lua = (_SCRIPTS_DIR / "common.lua").read_text()
        self._enqueue_script = self._load_script("enqueue")
        self._dequeue_script = self._load_script("dequeue")
        self._reclaim_script = self._load_script("reclaim")
        self._ack_script = self._load_script("ack")
        self._renew_script = self._load_script("renew")
        self._broker: redis_lib.Redis | None = self._connect_broker()

        if reclaim_interval is not None:
            self._register_reclaim_task()

    def task(self, fn: Callable | None = None, **task_options: Any) -> Any:
        """
        Decorator that registers a function as a VTRR-dispatchable Celery task.

        A VTRRQueue runs exactly one task: decorating a second function raises ValueError. Use one
        VTRRQueue (with its own `name` and `celery_queue`) per task.

        The first argument is always `self` (the bound Celery task), then any args/kwargs from .queue():

            @vtrr.task(autoretry_for=(TransientError,), max_retries=3,
                       retry_backoff=True)
            def my_task(self, task_id, x, y): ...
        """

        _ALLOWED_TASK_OPTIONS = {
            # execution limits
            "soft_time_limit",
            "time_limit",
            # retry
            "max_retries",
            "default_retry_delay",
            "autoretry_for",
            "retry_backoff",
            "retry_backoff_max",
            "retry_jitter",
            "retry_kwargs",
            # delivery
            "acks_late",
            "reject_on_worker_lost",
            # base class
            "base",
        }

        def decorator(f: Callable) -> VTRRTask:
            vtrr = self
            # Re-decorating the same function (e.g. a module reload) just replaces it
            if (
                self._task is not None
                and self._task.name != f"{f.__module__}.{f.__name__}"
            ):
                raise ValueError(
                    f"VTRRQueue {self._name!r} already runs task {self._task.name!r}; "
                    f"create a separate VTRRQueue to register {f.__module__}.{f.__name__}"
                )
            unsupported_task_options = set(task_options) - _ALLOWED_TASK_OPTIONS
            if unsupported_task_options:
                raise TypeError(
                    f"@vtrr.task does not support option(s): {', '.join(sorted(unsupported_task_options))}. "
                    f"Allowed: {', '.join(sorted(_ALLOWED_TASK_OPTIONS))}"
                )
            options = dict(task_options)
            options.pop("bind", None)
            base = _resolve_base(options.pop("base", None))
            task_name = f"{f.__module__}.{f.__name__}"

            @self._celery_app.task(name=task_name, bind=True, base=base, **options)
            def celery_wrapper(
                celery_task: Any,
                task_payload: dict | None = None,
                is_start: bool = False,
            ) -> None:
                dispatched = task_payload is None

                if dispatched:
                    result = vtrr._dequeue()
                    if result is None:
                        if is_start:
                            # Race: worker arrived before enqueue settled — retry once after a short delay.
                            celery_task.apply_async(
                                kwargs={"is_start": False},
                                queue=vtrr._celery_queue,
                                countdown=3,
                            )
                        return  # queue empty — stop draining, no reschedule
                    dequeued_name, args, kwargs, task_id, token, timeouts = result
                    task_payload = {
                        "task_name": dequeued_name,
                        "args": args,
                        "kwargs": kwargs,
                        "timeouts": timeouts,
                        "task_id": task_id,
                        "token": token,
                    }
                else:
                    # Retry attempt
                    dequeued_name = task_payload["task_name"]
                    args = task_payload["args"]
                    kwargs = task_payload["kwargs"]
                    task_id = task_payload.get("task_id")
                    token = task_payload.get("token")
                    if token and not vtrr._renew_lease(task_id, token):
                        # Lease expired and was reclaimed: the item belongs to another run now.
                        logger.warning(
                            "vtrr_queue: lease for %s lost before retry — dropping retry",
                            task_id,
                        )
                        return

                # Per-invocation stash (safe under threaded/gevent pools);
                # retry() reads this to re-run the same item.
                celery_task.request.vtrr_payload = task_payload
                # 0 on the first run; +1 each time the item's lease expired and it was reclaimed
                celery_task.request.vtrr_timeouts = task_payload.get("timeouts", 0)

                retrying = False
                try:
                    registered = vtrr._task
                    if registered is None or registered.name != dequeued_name:
                        logger.error(
                            "vtrr_queue: item for task %r does not match this queue's task %r — dropped",
                            dequeued_name,
                            registered.name if registered else None,
                        )
                        return
                    registered._fn(celery_task, *args, **kwargs)
                except Retry:
                    # Keep the lease: the retry run re-leases it (renew) and acks when done.
                    retrying = True
                    raise
                finally:
                    if token and not retrying:
                        # Ack even on failure: only process death / lease expiry redelivers.
                        # A failed ack just means the item is redelivered after the lease expires.
                        try:
                            vtrr.ack(task_id, token)
                        except Exception as err:
                            logger.error(
                                "vtrr_queue: could not ack %s: %s",
                                task_id,
                                err,
                                exc_info=True,
                            )
                    if dispatched:
                        # In case there's an exception, schedule next dequeue
                        vtrr._schedule_next(celery_task)

            wrapped = VTRRTask(f, self, celery_wrapper)
            self._task = wrapped
            return wrapped

        if fn is not None:
            return decorator(fn)
        return decorator

    # ------------------------------------------------------------------
    # Internal: setup
    # ------------------------------------------------------------------

    def _load_script(self, name: str) -> Any:
        src = self._common_lua + (_SCRIPTS_DIR / f"{name}.lua").read_text()
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
        self._enqueue_script(keys=self._enqueue_keys, args=argv)

    def _dequeue(self) -> tuple[str, list, dict, str, str, int] | None:
        """Lease the next item: (task_name, args, kwargs, task_id, lease_token, timeouts)."""
        raw = self._dequeue_script(
            keys=self._dequeue_keys,
            args=[
                self._lease_seconds,
                uuid.uuid4().hex,
            ],
        )
        if not raw:
            return None
        task_id, payload_bytes, token, timeouts = raw
        if isinstance(payload_bytes, (bytes, bytearray)):
            payload_bytes = payload_bytes.decode()
        payload = json.loads(payload_bytes)
        return (
            payload["task_name"],
            payload["args"],
            payload["kwargs"],
            _decode(task_id),
            _decode(token),
            int(timeouts),
        )

    # ------------------------------------------------------------------
    # Public: leases
    # ------------------------------------------------------------------

    def ack(self, task_id: str, token: str) -> bool:
        """Release a leased item. False (and no change) if the token doesn't match the current lease."""
        return bool(self._ack_script(keys=self._keys, args=[task_id, token]))

    def _renew_lease(self, task_id: str, token: str) -> bool:
        """Extend a held lease by `lease_seconds`. False if the token no longer matches."""
        return bool(
            self._renew_script(
                keys=self._keys, args=[task_id, token, self._lease_seconds]
            )
        )

    def reclaim(self) -> list[str]:
        """
        Re-enqueue (rescored) every item whose lease has expired. Returns the ids that were re-enqueued.
        Also runs automatically at the start of every dequeue.
        """
        ids = self._reclaim_script(keys=self._keys, args=[])
        return [_decode(i) for i in ids]

    def reclaim_and_dispatch(self) -> list[str]:
        """
        Reclaim expired leases.
        If a worker silently fails (OOM, restarts, etc) it may silently drop the item it was working on.
        Reclaim will check if the lease on that item has expired, then dispatch a new worker on it.

        Returns the ids that were re-enqueued.
        """
        # Reclaim expired leases
        reclaimed = self.reclaim()

        # Refresh/saturate scheduled workers if there are items in the queue.
        try:
            waiting = int(self._redis.zcard(self._queue_key))
        except Exception as err:
            logger.error(
                "vtrr_queue: could not read queue length: %s", err, exc_info=True
            )
            return reclaimed
        if waiting:
            if self._task is None:
                logger.error(
                    "vtrr_queue: %d item(s) waiting but no @vtrr.task is registered in this process",
                    waiting,
                )
            else:
                self._schedule_workers(waiting, self._task._celery_task)
        return reclaimed

    def reclaim_beat_schedule(self) -> dict[str, dict[str, Any]]:
        """
        The Celery Beat entry for periodic reclaim, or {} if `reclaim_interval` is unset.

        Merged into `celery_app.conf.beat_schedule` automatically at construction. Use this to
        re-add it if your app assigns `beat_schedule` wholesale after creating the VTRRQueue:

            app.conf.beat_schedule = {**my_entries, **vtrr.reclaim_beat_schedule()}
        """
        if self._reclaim_interval is None:
            return {}
        entry: dict[str, Any] = {
            "task": self._reclaim_task_name,
            "schedule": float(self._reclaim_interval),
        }
        if self._reclaim_queue:
            entry["options"] = {"queue": self._reclaim_queue}
        return {self._reclaim_task_name: entry}

    def _register_reclaim_task(self) -> None:
        # shared=False: bind to this app only. Celery's default shares the task with every
        # app finalized later in the process, which would run it against this queue's closure.
        @self._celery_app.task(
            name=self._reclaim_task_name, ignore_result=True, shared=False
        )
        def reclaim_task() -> None:
            self.reclaim_and_dispatch()

        schedule = dict(self._celery_app.conf.beat_schedule or {})
        schedule.update(self.reclaim_beat_schedule())
        self._celery_app.conf.beat_schedule = schedule

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
            dispatch_task.apply_async(
                kwargs={"is_start": True},
                queue=self._celery_queue,
            )

    def _schedule_next(self, current_celery_task: Any) -> None:
        """Schedule one more dequeue worker if tasks remain in the queue."""
        try:
            if self._redis.zcard(self._queue_key) > 0:
                current_celery_task.apply_async(queue=self._celery_queue)
        except Exception as err:
            logger.error(
                "vtrr_queue: could not schedule next dequeue worker: %s",
                err,
                exc_info=True,
            )

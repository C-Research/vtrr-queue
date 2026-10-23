# vtrr-queue

Virtual-time round-robin (VTRR) fair queue on Redis. Tasks from different users are interleaved so that no single user can starve others, regardless of how many tasks they submit. Each user's tasks are processed in proportion to their share of the queue.

Integrates with an existing Celery + Redis setup. The library owns the dequeue loop; your code just defines tasks and enqueues them.


## Requirements

- Python >= 3.12
- Redis (used for both the VTRR queue state and as the Celery broker)
- Celery >= 5.0
- Optional but recommended: Celery Beat (or any scheduler that can call `vtrr.reclaim_and_dispatch()`). Queueing and running tasks work without it, but recovering work from a dead worker when the queue is otherwise idle does not. See [Periodic reclaim](#periodic-reclaim-with-celery-beat)


## Installation

Install directly from GitHub until the package is published to a package registry:

```
pip install git+https://github.com/C-Research/vtrr-queue.git
```

Or pin a specific commit or tag for stability:

```
pip install git+https://github.com/C-Research/vtrr-queue.git@v0.1.0
```

Add to your `requirements.txt` or `pyproject.toml` dependencies the same way.


## How it works

```
User A submits 10 tasks ──┐
User B submits  2 tasks ──┼──► Redis sorted set (score = virtual time)
User C submits  5 tasks ──┘          │
                                     ▼
                          Your @vtrr.task Celery worker runs,
                          pops the highest-priority task from Redis,
                          and calls your function directly
```

When `.queue()` is called the library:
1. Atomically writes all tasks to Redis with fair virtual-time scores (Lua script, no race conditions).
2. Schedules `min(max_concurrency - workers_already_queued, num_tasks_just_enqueued)` Celery workers.

Each `@vtrr.task` is itself the Celery task — when a worker picks it up, it dequeues from Redis and calls your function in one hop. There is no intermediate dispatcher task.


## Setup

### 1. Create the VTRRQueue instance

Create a module that constructs the `VTRRQueue` instance. This module must be imported before Celery workers start (same requirement as Celery's own `@app.task` decorators).

```python
# myapp/vtrr.py
import redis
from celery import Celery
from vtrr_queue import VTRRQueue

celery_app = Celery(...)  # or import your existing app

vtrr = VTRRQueue(
    redis_client=redis.from_url("redis://localhost:6379/3"),
    celery_app=celery_app,
    name="files",                # namespaces Redis keys as vtrr:files:*
    celery_queue="vtrr_queue",   # name of the Celery queue workers will consume (separate from the round-robin queue)
    max_concurrency=8,           # cap on dequeue workers queued in the broker at once
)
```

Use a dedicated Redis DB index for the VTRR queue so its keys don't collide with your application cache, session store, or Celery broker.
#### VTRRQueue parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `redis_client` | `redis.Redis` | required | Redis client pointed at the VTRR database |
| `celery_app` | `Celery` | required | Your application's Celery instance |
| `name` | `str` | required | Unique name for this queue instance; namespaces all Redis keys as `vtrr:{name}:*` |
| `celery_queue` | `str` | `"vtrr_queue"` | Celery queue name for the dispatcher task |
| `max_concurrency` | `int` | `4` | Max dequeue workers held in the broker at once |
| `lease_seconds` | `int` | `3900` | How long a dequeued item stays leased before it is reclaimed. **Must exceed the task's hard `time_limit`.** See [Leasing](#leasing-and-at-least-once-delivery) |
| `reclaim_interval` | `float \| None` | `None` | Seconds between periodic reclaims run by Celery Beat. `None` disables it. See [Periodic reclaim](#periodic-reclaim-with-celery-beat) |
| `reclaim_queue` | `str \| None` | `None` | Celery queue the reclaim task is sent to. `None` uses your default queue. See [Periodic reclaim](#periodic-reclaim-with-celery-beat) |


### 2. Register a dedicated Celery worker

The VTRR dispatcher runs on its own Celery queue. Start a worker that consumes only that queue so it doesn't compete with your other task queues:

```
celery -A myapp worker -Q vtrr_queue --concurrency=8 --loglevel=info
```

The `--concurrency` here is the OS-level worker pool size. `max_concurrency` in `VTRRQueue` controls how many dispatcher tasks are *pre-queued* in the broker at once, which is this concurrency value multiplied by the number of worker instances you deploy.

#### One `celery_queue` per `VTRRQueue` instance

Give each `VTRRQueue` instance its own `celery_queue` name. Otherwise, if two or more instances share the same name, the library may under-schedule the celery workers.

```python
# Good — each queue manages its own worker budget independently
files_vtrr = VTRRQueue(..., name="files",  celery_queue="vtrr_files",  max_concurrency=8)
search_vtrr = VTRRQueue(..., name="search", celery_queue="vtrr_search", max_concurrency=4)
```

Start a worker per queue so you can also size the pools independently:

```
celery -A myapp worker -Q vtrr_files  --concurrency=8 --loglevel=info
celery -A myapp worker -Q vtrr_search --concurrency=4 --loglevel=info
```

If you genuinely want both queues served by a single shared worker pool, you can point both workers at both queues (`-Q vtrr_files,vtrr_search`), but keep the queue names distinct so scheduling counts stay accurate.


### 3. Define tasks

Import the `vtrr` instance and decorate functions with `@vtrr.task`. The decorator accepts many of the same keyword arguments as Celery's `@app.task`:

```python
# myapp/tasks.py
from myapp.vtrr import vtrr
from myapp.celery_utils import LogErrorsTask

# No options — plain decorator
@vtrr.task(
    base=LogErrorsTask,
    max_retries=3,
    soft_time_limit=3600,
)
def process_file(self, task_id: str, file_key: str, dataset_id: str, force_ocr: bool = False):
    try:
        ...
    except TransientError as exc:
        # Retry will requeue the specific task to the broker
        raise self.retry(exc=exc, countdown=10)
```

`self` is the bound Celery task and is always the first argument, followed by any args/kwargs passed to `.queue()`. bound is always True despite what you may set in the decorator.

Each task gets its own Celery task registration with its own retry and timeout settings.


### 4. Enqueue tasks

Call `.queue()` from anywhere in your application to enqueue the task in the round-robin for celery to run.

```python
from myapp.tasks import process_file

# Bulk enqueue — all tasks from one partition in a single Redis round-trip
process_file.queue(
    partition_key="u_123",
    tasks=[
        {"weight": 4, "args": ["s3://bucket/a.pdf"], "kwargs": {"dataset_id": "d_1", "force_ocr": True}},
        {"weight": 2, "args": ["s3://bucket/b.pdf"], "kwargs": {"dataset_id": "d_2"}},
        {"weight": 2, "args": ["s3://bucket/c.pdf"], "kwargs": {"dataset_id": "d_3"}},
    ],
)
```

#### .queue() parameters

| Parameter | Type | Description |
|---|---|---|
| `partition_key` | `str` | Required. Identifies whose virtual-time slot to use. |
| `tasks` | `list[dict]` | Required. Each dict has optional `"weight"` (int), `"args"` (list), and `"kwargs"` (dict). |

Each task dict may also include an `"id"` key to supply a stable task ID; otherwise a UUID is generated.

`"weight"` defaults to `1`, which gives standard one-turn-per-task round-robin fairness. A weight of `N` advances the partition's virtual time by `N` after that task is enqueued, yielding up to `N` turns to other partitions before the next task in this partition is served. Use higher weights for tasks that are known to be proportionally more expensive.


## Celery task options

`@vtrr.task` accepts an explicit subset of `@celery_app.task()` options. Passing anything outside this list raises a `TypeError` at decoration time.

| Option | Description |
|---|---|
| `base` | Custom base task class (e.g. for structured error reporting) |
| `max_retries` | Number of times to retry on exception |
| `default_retry_delay` | Seconds to wait before retrying (default: 180) |
| `autoretry_for` | Tuple of exception types to retry on automatically |
| `retry_backoff` | Enable exponential backoff between retries |
| `retry_backoff_max` | Cap on backoff delay in seconds |
| `retry_jitter` | Add random jitter to backoff delays |
| `retry_kwargs` | Dict of kwargs passed to `self.retry()`, e.g. `{"countdown": 10}` |
| `soft_time_limit` | Seconds before a `SoftTimeLimitExceeded` is raised in the worker |
| `time_limit` | Hard kill timeout in seconds |
| `acks_late` | Ack the Celery trigger after the task finishes. Optional: leasing does not depend on it |
| `reject_on_worker_lost` | Requeue the trigger if the worker process is killed mid-task. Optional: leasing does not depend on it |

A `VTRRQueue` runs exactly one task: decorating a second function on the same instance raises `ValueError`. Every Celery trigger on a queue dequeues whatever item is next, so the Celery options (time limits, retries, `base`) must belong to the one task that every item runs. For several task types, create one `VTRRQueue` per task, each with its own `name` and `celery_queue`.


## Django-specific setup

A complete Django project integration:

```python
# myproject/celery.py  (standard Celery Django setup)
import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")
app = Celery("myproject")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
```

```python
# myproject/settings.py
CACHES = {
    # ... your existing cache entries ...
    "vtrr": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://localhost:6379",
        "OPTIONS": {
            "DB": 3,                                        # dedicated database index
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
        },
    },
}

VTRR_CELERY_SCHEDULER_QUEUE = "vtrr_queue"
VTRR_MAX_CONCURRENCY = 8
```

```python
# myapp/vtrr.py
from django.conf import settings
from django_redis import get_redis_connection
from myproject.celery import app as celery_app
from vtrr_queue import VTRRQueue

vtrr = VTRRQueue(
    redis_client=get_redis_connection("vtrr"),  # uses the "vtrr" CACHES entry
    celery_app=celery_app,
    name="files",
    celery_queue=settings.VTRR_CELERY_SCHEDULER_QUEUE,
    max_concurrency=settings.VTRR_MAX_CONCURRENCY,
)
```

`get_redis_connection` is the recommended approach over a bare `redis.from_url()` in Django projects since it integrates with Django's connection management.

```python
# myapp/apps.py
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import myapp.tasks  # registers @vtrr.task functions with the dispatcher if needed
```

```python
# myapp/tasks.py
from myapp.vtrr import vtrr
from myapp.celery_utils import LogErrorsTask

@vtrr.task(
    base=LogErrorsTask,
    max_retries=3,
    retry_kwargs={"countdown": 10},
    soft_time_limit=3600,
)
def process_upload(self, task_id: str, file_key: str, dataset_id: str):
    ...
```

```python
# myapp/views.py
from myapp.tasks import process_upload

class UploadView(View):
    def post(self, request):
        files = request.FILES.getlist("files")
        process_upload.queue(
            partition_key=str(request.user.id),
            tasks=[
                {"weight": 1, "args": [f.name], "kwargs": {"dataset_id": request.POST["dataset_id"]}}
                for f in files
            ],
        )
        return JsonResponse({"queued": len(files)})
```


## Leasing and at-least-once delivery

Dequeuing an item does not delete it; it **leases** it. The item moves to an in-flight set with an expiry of `redis_time + lease_seconds` (Redis `TIME`, so worker clock skew is irrelevant) and its lease token is stored. The `@vtrr.task` wrapper acks the lease when your function returns. If the worker dies first, the lease expires and the next dequeue (or a call to `vtrr.reclaim()`, or the [periodic reclaim](#periodic-reclaim-with-celery-beat)) puts the item back in the queue, **rescored** under VTRR as if freshly enqueued; it does not keep its old position. Each reclaim adds one to the item's **timeouts** count, which is never reset.

Delivery is therefore **at-least-once: task bodies must be idempotent.** Inside a task, `self.request.vtrr_timeouts` is how many times this item's lease has expired and been reclaimed: 0 on the first run, +1 each time it is run again after a timeout. The library does not cap timeouts; your task decides when to give up (see [Timeout limits](#timeout-limits)).

The wrapper acks in a `finally`, so a Python exception does not redeliver forever; only process death or lease expiry does. This matches Celery's default `task_acks_on_failure_or_timeout=True`. If you use the low-level API directly:

```python
task_name, args, kwargs, task_id, token, timeouts = vtrr._dequeue()
try:
    ...
finally:
    vtrr.ack(task_id, token)   # False if the lease already expired and was re-granted; harmless
```

`self.retry()` / `autoretry_for` keep the lease: the wrapper does not ack when a task retries, and the retry run re-leases the item (expiry reset to `redis_time + lease_seconds`) and acks when it finishes. If the lease was already reclaimed during the retry delay (for example a `countdown` longer than `lease_seconds`), the item belongs to a newer delivery and the retry run is dropped instead of executing twice.

`ack` is fenced: it only succeeds when the token matches the current lease, so a slow worker whose lease was reclaimed cannot ack the new holder's lease.

### Celery configuration and timing

Recovery does not depend on Celery's delivery guarantees. The Celery message that wakes a worker (the "trigger") carries no payload; the work item lives in Redis under its lease. If a worker dies, the item comes back when its lease expires, and [periodic reclaim](#periodic-reclaim-with-celery-beat) wakes a worker for it. You do not need `acks_late`, `reject_on_worker_lost`, or a raised `visibility_timeout` for this. (They remain available task options. `reject_on_worker_lost` in particular does not speed recovery: the requeued trigger runs while the lease is still held, so it cannot reclaim the item.)

Required order:

```
soft_time_limit < time_limit (hard) < lease_seconds
```

For an hour-long task: soft 3600, hard 3720, lease 3900.

```python
vtrr = VTRRQueue(..., lease_seconds=3900, reclaim_interval=600)

@vtrr.task(soft_time_limit=3600, time_limit=3720)
def process_file(self, ...):
    ...
```

- **Hard `time_limit` is required.** `soft_time_limit` only raises an exception, which blocking C/IO code can delay or swallow. The hard limit kills the process, so by the time a lease expires the original holder is gone and reclaim rarely causes concurrent executions.
- **`lease_seconds` > hard limit**, for the reason above.
- A recovered item waits for its lease to expire, so worst-case recovery time is about `lease_seconds + reclaim_interval`.
- If you do enable `acks_late`, the Redis transport's `visibility_timeout` (default 3600s, broker-wide) applies to it: a trigger held unacked longer is redelivered while still running. The lease token fences the duplicate, but keep `visibility_timeout` above your longest task.

### Periodic reclaim with Celery Beat

Reclaim also runs at the start of every dequeue, but that is not enough on its own: if the queue is idle, or every trigger for waiting items has been lost (a worker died between receiving its trigger and dequeuing), no dequeue happens and the item sits there. `vtrr.reclaim_and_dispatch()` reclaims expired leases and then schedules dequeue workers for whatever is waiting (up to `max_concurrency` minus the triggers already in the broker). It is cheap: when nothing has expired and the queue is empty it is one Redis call that returns immediately.

Pass `reclaim_interval` to run it on Celery Beat:

```python
vtrr = VTRRQueue(..., reclaim_interval=600, reclaim_queue="celery")
```

At construction the library registers a task named `vtrr_queue.reclaim.{name}` and merges its entry into `celery_app.conf.beat_schedule`. You still have to run Beat (`celery -A myapp beat`), and the process that executes the task must have imported your `@vtrr.task` functions, which it uses as triggers.

- **`reclaim_queue`**: leave it unset to use your default Celery queue, or name a queue that some worker consumes. Don't send it to `celery_queue` if those workers are usually busy with long tasks; it would wait behind them.
- **If your app assigns `beat_schedule` wholesale after creating the `VTRRQueue`**, it overwrites the entry. Merge `vtrr.reclaim_beat_schedule()` back in:

  ```python
  app.conf.beat_schedule = {**my_entries, **vtrr.reclaim_beat_schedule()}
  ```
- **Without Beat**, call `vtrr.reclaim_and_dispatch()` from whatever scheduler you have (cron, a management command, etc.).

### Timeout limits

The library never gives up on an item: every expired lease is reclaimed and re-enqueued. To cap redeliveries, check the timeouts count in your task and return early; the wrapper then acks the item and it is dropped.

```python
MAX_TIMEOUTS = 3

@vtrr.task(soft_time_limit=3600, time_limit=3720)
def process_file(self, ...):
    if self.request.vtrr_timeouts >= MAX_TIMEOUTS:
        log_poison_item(...)   # your own dead-letter handling
        return
    ...
```


## Redis keys

Each `VTRRQueue` instance manages these keys, namespaced under `vtrr:{name}:*`.

| Key | Type | Description |
|---|---|---|
| `vtrr:{name}:queue` | Sorted Set | Task IDs scored by virtual time; lowest score = highest priority |
| `vtrr:{name}:current_virtual_time` | String | Virtual time of the last dequeued task; resets to 0 when queue drains |
| `vtrr:{name}:partition_virtual_time` | Hash | Per-partition virtual time counter; deleted when queue drains |
| `vtrr:{name}:task` | Hash | `task_id → JSON payload` for queued items; deleted when queue drains |
| `vtrr:{name}:meta` | Hash | `task_id → "weight:partition_key"`, kept until ack so reclaim can rescore |
| `vtrr:{name}:inflight` | Sorted Set | Leased task IDs scored by lease expiry (Redis time) |
| `vtrr:{name}:inflight_task` | Hash | `task_id → JSON payload` for leased items |
| `vtrr:{name}:lease` | Hash | `task_id → lease token` |
| `vtrr:{name}:timeouts` | Hash | `task_id → times its lease expired and was reclaimed`; cleared on ack |

Multiple `VTRRQueue` instances in the same application can safely share a Redis database as long as they have distinct `name` values.

All enqueue, dequeue, ack and reclaim operations are atomic Lua scripts, so concurrent writers and workers are safe.

# Development

Run `poetry install`

Tests: `poetry run pytest tests`
# vtrr-queue

Virtual-time round-robin (VTRR) fair queue on Redis. Tasks from different users are interleaved so that no single user can starve others, regardless of how many tasks they submit. Each user's tasks are processed in proportion to their share of the queue.

Integrates with an existing Celery + Redis setup. The library owns the dequeue loop; your code just defines tasks and enqueues them.


## Requirements

- Python >= 3.10, <= 3.11.5
- Redis (used for both the VTRR queue state and as the Celery broker)
- Celery >= 5.0


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
| `celery_queue` | `str` | `"vtrr_queue"` | Celery queue name for the dispatcher task |
| `max_concurrency` | `int` | `4` | Max dequeue workers held in the broker at once |


### 2. Register a dedicated Celery worker

The VTRR dispatcher runs on its own Celery queue. Start a worker that consumes only that queue so it doesn't compete with your other task queues:

```
celery -A myapp worker -Q vtrr_queue --concurrency=8 --loglevel=info
```

The `--concurrency` here is the OS-level worker pool size. `max_concurrency` in `VTRRQueue` controls how many dispatcher tasks are *pre-queued* in the broker at once, which is this concurrency value multiplied by the number of worker instances you deploy.


### 3. Define tasks

Import the `vtrr` instance and decorate functions with `@vtrr.task`. The decorator accepts the same keyword arguments as Celery's `@app.task`:

```python
# myapp/tasks.py
from myapp.vtrr import vtrr
from myapp.celery_utils import LogErrorsTask

# No options — plain decorator
@vtrr.task(
    base=LogErrorsTask,
    max_retries=3,
    soft_time_limit=3600,
    acks_late=True,
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

`@vtrr.task` accepts the same keyword arguments as `@celery_app.task()`. Common options:

| Option | Description |
|---|---|
| `base` | Custom base task class (e.g. for structured error reporting) |
| `max_retries` | Number of times to retry on exception |
| `retry_kwargs` | Dict of kwargs passed to `self.retry()`, e.g. `{"countdown": 10}` |
| `soft_time_limit` | Seconds before a `SoftTimeLimitExceeded` is raised in the worker |
| `time_limit` | Hard kill timeout in seconds |
| `acks_late` | Only acknowledge the message after the task completes |

Since each `@vtrr.task` becomes its own Celery task, different task functions can have different retry policies and time limits.


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


## Redis keys

The library manages four keys in Redis. These are fixed and not configurable.

| Key | Type | Description |
|---|---|---|
| `vtrr:queue` | Sorted Set | Task IDs scored by virtual time; lowest score = highest priority |
| `vtrr:current_virtual_time` | String | Virtual time of the last dequeued task; resets to 0 when queue drains |
| `vtrr:user_virtual_time` | Hash | Per-user virtual time counter; deleted when queue drains |
| `vtrr:task` | Hash | `task_id → JSON payload`; deleted when queue drains |

All enqueue and dequeue operations are atomic Lua scripts, so concurrent writers and workers are safe.

# Development

Run `poetry install`

Tests: `poetry run pytest tests`
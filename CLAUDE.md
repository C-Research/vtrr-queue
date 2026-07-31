# CLAUDE.md

Read README.md for full usage documentation, API reference, and Django integration examples.

## Keeping docs current

After any edit, check whether it makes anything in CLAUDE.md or README.md factually wrong or misleading. If so, update only the affected parts. Do not add new documentation for every change — only update when something that is already written has become incorrect or incomplete in a way that would mislead a future developer. New features or API additions warrant README and CLAUDE.md updates only if they are central to how the library is used.

## What this repo is

A standalone Python library (`vtrr-queue`) that will be imported by a separate Django/Celery application. It is not a Django app itself — no models, no views, no settings. It has no knowledge of the consuming project's domain.

The consuming project lives in a separate repo. This library is currently installed via GitHub pip URL; AWS CodeArtifact is the eventual target.

## Repo layout

```
src/vtrr_queue/
  __init__.py       # public API: VTRRQueue, VTRRTask
  queue.py          # all logic lives here — single file by design
  py.typed          # PEP 561 marker
  scripts/
    enqueue.lua     # bulk enqueue, called by VTRRTask.queue()
    dequeue.lua     # pops one task, called by each @vtrr.task celery wrapper
tests/              # empty — needs fakeredis-based tests
```

## Key design constraints

- **Single source file** (`queue.py`) — do not split into multiple modules unless the file becomes unmanageable.
- **No Django dependency** — the library must work with plain Celery + redis-py. Django integration is the consumer's responsibility (`get_redis_connection`, settings wiring, etc.).
- **No enqueue.lua modification without updating dequeue.lua** — the two scripts share assumptions about key layout and must stay consistent. Both are atomic Lua scripts; never replace them with multi-command Python.
- **`@vtrr.task` IS the Celery task** — the decorator wraps the user function in a Celery task whose body calls `vtrr._dequeue()` then invokes the function directly. One broker hop total; no intermediate dispatcher task.
- **`celery_queue` is passed explicitly** in every `apply_async` call, bypassing the consumer's `task_routes`.

## Redis key contract

Four fixed keys — do not rename without updating both Lua scripts and the README:

| Key | Structure |
|---|---|
| `vtrr:queue` | Sorted set: `task_id → virtual_time` |
| `vtrr:current_virtual_time` | String |
| `vtrr:user_virtual_time` | Hash: `user_id → virtual_time` |
| `vtrr:task` | Hash: `task_id → JSON payload` |

JSON payload shape: `{"task_name": str, "task_id": str, "args": list, "kwargs": dict}`

All four keys are deleted/reset when the queue drains (handled in `dequeue.lua`).

## Worker scheduling logic

`_schedule_workers(num_tasks)` schedules `min(max_concurrency - workers_scheduled, num_tasks)` Celery tasks after every `.queue()` call. `_get_workers_scheduled()` counts broker list lengths across Celery's four Redis priority suffixes (`""`, `":3"`, `":6"`, `":9"`). It returns `0` on any error so scheduling may over-fire slightly — this is intentional and safe.

## Development setup

```
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Tests use `fakeredis`. A real Celery app is needed to test dispatch end-to-end; unit tests should mock `celery_app` and assert on `apply_async` calls.

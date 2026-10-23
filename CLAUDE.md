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
    common.lua      # prelude prepended to every script: vtrr_enqueue (scoring) + vtrr_reclaim
    enqueue.lua     # bulk enqueue, called by VTRRTask.queue()
    dequeue.lua     # reclaims, then leases one task; called by each @vtrr.task celery wrapper
    reclaim.lua     # public VTRRQueue.reclaim()
    ack.lua         # fenced ack of a lease, VTRRQueue.ack()
    renew.lua       # fenced re-lease used by retry runs, VTRRQueue._renew_lease()
tests/              # fakeredis-based tests
```

## Key design constraints

- **Single source file** (`queue.py`) — do not split into multiple modules unless the file becomes unmanageable.
- **No Django dependency** — the library must work with plain Celery + redis-py. Django integration is the consumer's responsibility (`get_redis_connection`, settings wiring, etc.).
- **No script modification without checking the others** — the scripts share `common.lua` and assumptions about key layout and must stay consistent. Both are atomic Lua scripts; never replace them with multi-command Python.
- **`@vtrr.task` IS the Celery task** — the decorator wraps the user function in a Celery task whose body calls `vtrr._dequeue()` then invokes the function directly. One broker hop total; no intermediate dispatcher task.
- **One task per `VTRRQueue`** — `@vtrr.task` raises `ValueError` for a second function. Triggers are interchangeable and run whatever item they dequeue, so a mixed queue would run items under the wrong task's Celery options. `vtrr._task` is the single registered task; the item payload's `task_name` is checked against it.
- **`celery_queue` is passed explicitly** in every `apply_async` call, bypassing the consumer's `task_routes`.

## Redis key contract

Each `VTRRQueue` instance namespaces its keys under `vtrr:{name}:*`. Do not rename without updating the Lua scripts and the README. Every script receives the same `KEYS` list (order defined in `common.lua`).

| Key | Structure |
|---|---|
| `vtrr:{name}:queue` | Sorted set: `task_id → virtual_time` |
| `vtrr:{name}:current_virtual_time` | String |
| `vtrr:{name}:partition_virtual_time` | Hash: `partition_key → virtual_time` |
| `vtrr:{name}:task` | Hash: `task_id → JSON payload` |

JSON payload shape: `{"task_name": str, "args": list, "kwargs": dict}`

The first four keys are deleted/reset when the queue drains (handled in `dequeue.lua`). Lease state (`meta`, `inflight`, `inflight_task`, `lease`, `attempts`; see README) is untouched by a drain. Dequeue grants a lease rather than deleting the item; reclaim rescores expired items through the same `vtrr_enqueue` function a fresh enqueue uses — never duplicate that logic.

## Task weights

Each task dict passed to `.queue()` may include a `"weight"` key (int, default `1`). Weight controls how much virtual time is consumed after enqueuing that task: a weight of `N` advances the partition's virtual-time counter by `N`, yielding up to `N` turns to other partitions before the next task in this partition is served. Weight `1` is the standard turn-based round-robin.

## Worker scheduling logic

`_schedule_workers(num_tasks)` schedules `min(max_concurrency - workers_scheduled, num_tasks)` Celery tasks after every `.queue()` call. `_get_workers_scheduled()` counts broker list lengths across Celery's four Redis priority suffixes (`""`, `":3"`, `":6"`, `":9"`). It returns `0` on any error so scheduling may over-fire slightly — this is intentional and safe.

## Periodic reclaim

Recovery of work from dead workers must not depend on Celery's `acks_late` / `reject_on_worker_lost` (they stay allowed task options but the library never requires them). Trigger messages carry no payload, so a lost trigger loses nothing; `reclaim_and_dispatch()` (reclaim expired leases, then `_schedule_workers(zcard(queue))` using any registered task as the trigger) is what wakes workers back up. With `reclaim_interval` set, the constructor registers the `vtrr_queue.reclaim.{name}` Celery task and merges its entry into `celery_app.conf.beat_schedule`; `beat_schedule()` returns the entry for apps that assign the schedule wholesale.

## Development setup

```
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Tests use `fakeredis`. A real Celery app is needed to test dispatch end-to-end; unit tests should mock `celery_app` and assert on `apply_async` calls.

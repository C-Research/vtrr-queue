"""VTRR queue test suite."""

import json
from unittest.mock import MagicMock

import fakeredis
import pytest

import vtrr_queue.queue as vtrr_module
from vtrr_queue.queue import VTRRQueue

SCRIPTS_DIR = vtrr_module._SCRIPTS_DIR


def make_argv(task_name, items):
    """Build flat (partition_key, task_id, weight, payload) quads for enqueue.lua.

    items: iterable of (partition_key, task_id, args, kwargs).
    """
    argv = []
    for partition_key, task_id, args, kwargs in items:
        payload = json.dumps({"task_name": task_name, "args": args, "kwargs": kwargs})
        argv += [partition_key, task_id, 1, payload]
    return argv


@pytest.fixture
def enqueue_script(vtrr):
    """Bound enqueue callable — takes argv, keys are baked in."""
    return lambda argv: vtrr._enqueue_script(keys=vtrr._enqueue_keys, args=argv)


@pytest.fixture
def dequeue_script(vtrr):
    """Bound dequeue callable — no arguments needed."""
    return lambda: vtrr._dequeue_script(keys=vtrr._dequeue_keys, args=[])


def drain_all(dequeue_script):
    """Pop everything, returning the ordered list of task_ids (bytes)."""
    order = []
    while True:
        result = dequeue_script()
        if not result:
            break
        order.append(result[0])
    return order


class TestRoundRobin:
    def test_enqueue_assigns_incrementing_virtual_time(
        self, vtrr, redis_client, enqueue_script
    ):
        enqueue_script(
            make_argv(
                "t",
                [
                    ("A", "a1", [], {}),
                    ("A", "a2", [], {}),
                    ("A", "a3", [], {}),
                ],
            )
        )
        scores = redis_client.zrange(vtrr._queue_key, 0, -1, withscores=True)
        assert scores == [(b"a1", 1.0), (b"a2", 2.0), (b"a3", 3.0)]
        assert redis_client.hget(vtrr._partitions_vt_key, "A") == b"4"

    def test_payload_round_trips(self, enqueue_script, dequeue_script):
        enqueue_script(make_argv("myapp.task", [("A", "a1", [1, 2], {"k": "v"})]))
        member, payload = dequeue_script()
        assert member == b"a1"
        assert json.loads(payload) == {
            "task_name": "myapp.task",
            "args": [1, 2],
            "kwargs": {"k": "v"},
        }

    def test_dequeue_pops_smallest_virtual_time_first(
        self, enqueue_script, dequeue_script
    ):
        enqueue_script(
            make_argv(
                "t",
                [
                    ("A", "a1", [], {}),
                    ("A", "a2", [], {}),
                    ("A", "a3", [], {}),
                ],
            )
        )
        assert drain_all(dequeue_script) == [b"a1", b"a2", b"a3"]

    def test_fairness_interleaves_across_users(self, enqueue_script, dequeue_script):
        # A enqueues three, B one just after. B (vt 1) shares the first cohort
        # with a1 (vt 1), so it is served second, not stuck behind a2/a3.
        enqueue_script(
            make_argv(
                "t",
                [
                    ("A", "a1", [], {}),
                    ("A", "a2", [], {}),
                    ("A", "a3", [], {}),
                ],
            )
        )
        enqueue_script(make_argv("t", [("B", "b1", [], {})]))
        assert drain_all(dequeue_script) == [b"a1", b"b1", b"a2", b"a3"]

    def test_current_vt_tracks_last_dequeue(
        self, vtrr, redis_client, enqueue_script, dequeue_script
    ):
        enqueue_script(
            make_argv(
                "t",
                [
                    ("A", "a1", [], {}),
                    ("A", "a2", [], {}),
                ],
            )
        )
        dequeue_script()  # pops a1 (vt 1); queue not empty
        assert float(redis_client.get(vtrr._current_vt_key)) == 1.0

    def test_drain_resets_all_state(self, vtrr, redis_client, enqueue_script, dequeue_script):
        enqueue_script(make_argv("t", [("A", "a1", [], {})]))
        dequeue_script()
        assert redis_client.get(vtrr._current_vt_key) == b"0"
        assert redis_client.exists(vtrr._task_key) == 0
        assert redis_client.exists(vtrr._partitions_vt_key) == 0

    def test_dequeue_empty_queue_returns_empty(self, dequeue_script):
        assert dequeue_script() == []

    def test_weight_advances_partition_vt_by_weight(self, vtrr, redis_client, enqueue_script):
        argv = []
        for task_id, weight in [("a1", 2), ("a2", 1)]:
            payload = json.dumps({"task_name": "t", "args": [], "kwargs": {}})
            argv += ["A", task_id, weight, payload]
        enqueue_script(argv)
        scores = redis_client.zrange(vtrr._queue_key, 0, -1, withscores=True)
        # a1 at vt=1, a2 at vt=1+2=3 (not 2)
        assert scores == [(b"a1", 1.0), (b"a2", 3.0)]
        # partition_vt = 3 + 1 = 4
        assert redis_client.hget(vtrr._partitions_vt_key, "A") == b"4"

    def test_heavier_weight_yields_more_turns_to_other_partition(
        self, enqueue_script, dequeue_script
    ):
        # A's tasks have weight=2, B's have weight=1.
        # After draining a1 (vt=1), B gets b1 (vt=1) and b2 (vt=2) before A's a2 (vt=3).
        argv = []
        for task_id, weight in [("a1", 2), ("a2", 2)]:
            payload = json.dumps({"task_name": "t", "args": [], "kwargs": {}})
            argv += ["A", task_id, weight, payload]
        for task_id in ["b1", "b2", "b3"]:
            payload = json.dumps({"task_name": "t", "args": [], "kwargs": {}})
            argv += ["B", task_id, 1, payload]
        enqueue_script(argv)
        assert drain_all(dequeue_script) == [b"a1", b"b1", b"b2", b"a2", b"b3"]


class TestMultipleInstances:
    def test_two_instances_use_separate_redis_keys(self, redis_client, celery_app):
        files_vtrr = VTRRQueue(
            redis_client=redis_client, celery_app=celery_app, name="files"
        )
        search_vtrr = VTRRQueue(
            redis_client=redis_client, celery_app=celery_app, name="search"
        )
        assert files_vtrr._queue_key != search_vtrr._queue_key
        assert files_vtrr._queue_key == "vtrr:files:queue"
        assert search_vtrr._queue_key == "vtrr:search:queue"

    def test_two_instances_do_not_share_queue_state(self, redis_client, celery_app):
        files_vtrr = VTRRQueue(
            redis_client=redis_client, celery_app=celery_app, name="files"
        )
        files_vtrr._broker = fakeredis.FakeStrictRedis(decode_responses=False)
        search_vtrr = VTRRQueue(
            redis_client=redis_client, celery_app=celery_app, name="search"
        )
        search_vtrr._broker = fakeredis.FakeStrictRedis(decode_responses=False)

        @files_vtrr.task
        def process_file(self, name):
            pass

        @search_vtrr.task
        def run_search(self, query):
            pass

        process_file.queue(partition_key="u1", tasks=[{"id": "f1", "args": ["a.pdf"]}])

        # files queue has one item; search queue is empty
        assert redis_client.zcard(files_vtrr._queue_key) == 1
        assert redis_client.zcard(search_vtrr._queue_key) == 0

    def test_dequeue_only_pops_from_own_namespace(self, redis_client, celery_app):
        files_vtrr = VTRRQueue(
            redis_client=redis_client, celery_app=celery_app, name="files"
        )
        files_vtrr._broker = fakeredis.FakeStrictRedis(decode_responses=False)
        search_vtrr = VTRRQueue(
            redis_client=redis_client, celery_app=celery_app, name="search"
        )
        search_vtrr._broker = fakeredis.FakeStrictRedis(decode_responses=False)

        files_ran = []
        search_ran = []

        @files_vtrr.task
        def process_file(self, name):
            files_ran.append(name)

        @search_vtrr.task
        def run_search(self, query):
            search_ran.append(query)

        process_file.queue(
            partition_key="u1",
            tasks=[{"kwargs": {"name": "a.pdf"}}, {"kwargs": {"name": "b.pdf"}}],
        )
        run_search.queue(
            partition_key="org1",
            tasks=[{"kwargs": {"query": "hello"}}],
        )

        process_file._celery_task.apply(kwargs={"is_start": True})
        run_search._celery_task.apply(kwargs={"is_start": True})

        assert sorted(files_ran) == ["a.pdf", "b.pdf"]
        assert search_ran == ["hello"]


class TestCeleryScheduling:
    def test_get_workers_scheduled_sums_priority_queues(self, vtrr):
        vtrr._broker.rpush("vtrr_queue", "m1", "m2")
        vtrr._broker.rpush("vtrr_queue:3", "m3")
        assert vtrr._get_workers_scheduled() == 3

    def test_schedule_workers_caps_at_remaining_concurrency(self, vtrr, monkeypatch):
        monkeypatch.setattr(vtrr, "_get_workers_scheduled", lambda: 1)
        task = MagicMock()
        vtrr._schedule_workers(num_tasks=10, dispatch_task=task)
        assert task.apply_async.call_count == 3  # max_concurrency(4) - 1

    def test_schedule_workers_noop_when_saturated(self, vtrr, monkeypatch):
        monkeypatch.setattr(vtrr, "_get_workers_scheduled", lambda: 4)
        task = MagicMock()
        vtrr._schedule_workers(num_tasks=5, dispatch_task=task)
        assert task.apply_async.call_count == 0

    def test_schedule_next_reschedules_when_queue_nonempty(self, vtrr):
        vtrr._redis.zadd(vtrr._queue_key, {"t1": 1})
        task = MagicMock()
        vtrr._schedule_next(task)
        task.apply_async.assert_called_once()

    def test_schedule_next_noop_when_queue_empty(self, vtrr):
        task = MagicMock()
        vtrr._schedule_next(task)
        task.apply_async.assert_not_called()


def kick(task):
    """Run one dispatch cycle synchronously (eager). Drains via _schedule_next."""
    task._celery_task.apply(kwargs={"is_start": True})


class TestCeleryTaskExec:
    def test_end_to_end_fair_drain(self, vtrr, monkeypatch):
        ran = []

        @vtrr.task
        def work(self, tag):
            ran.append(tag)

        monkeypatch.setattr(vtrr, "_schedule_workers", lambda *a, **k: None)
        work.queue(
            partition_key="A",
            tasks=[
                {"id": "a1", "kwargs": {"tag": "A1"}},
                {"id": "a2", "kwargs": {"tag": "A2"}},
                {"id": "a3", "kwargs": {"tag": "A3"}},
            ],
        )
        work.queue(partition_key="B", tasks=[{"id": "b1", "kwargs": {"tag": "B1"}}])
        kick(work)
        assert ran == ["A1", "B1", "A2", "A3"]

    def test_each_task_runs_exactly_once(self, vtrr, monkeypatch):
        seen = []

        @vtrr.task
        def work(self, n):
            seen.append(n)

        monkeypatch.setattr(vtrr, "_schedule_workers", lambda *a, **k: None)
        work.queue(partition_key="A", tasks=[{"kwargs": {"n": i}} for i in range(5)])
        kick(work)
        assert sorted(seen) == [0, 1, 2, 3, 4]

    def test_args_and_kwargs_thread_through(self, vtrr, monkeypatch):
        got = []

        @vtrr.task
        def work(self, a, b, c=None):
            got.append((a, b, c))

        monkeypatch.setattr(vtrr, "_schedule_workers", lambda *a, **k: None)
        work.queue(partition_key="A", tasks=[{"args": [1, 2], "kwargs": {"c": 3}}])
        kick(work)
        assert got == [(1, 2, 3)]

    def test_direct_payload_runs_without_rescheduling(self, vtrr, monkeypatch):
        ran = []

        @vtrr.task
        def work(self, x, y=0):
            ran.append((x, y))

        sched = MagicMock()
        monkeypatch.setattr(vtrr, "_schedule_next", sched)
        payload = {"task_name": work.name, "args": [5], "kwargs": {"y": 7}}
        # task_payload provided => the "re-run myself" path; must NOT drain more.
        work._celery_task.apply(kwargs={"task_payload": payload})
        assert ran == [(5, 7)]
        sched.assert_not_called()

    def test_is_start_retries_once_on_empty_queue(self, vtrr, monkeypatch):
        @vtrr.task
        def work(self):
            pass

        monkeypatch.setattr(vtrr, "_dequeue", lambda: None)
        apply_async = MagicMock()
        monkeypatch.setattr(work._celery_task, "apply_async", apply_async)
        work._celery_task.apply(kwargs={"is_start": True})

        apply_async.assert_called_once()
        _, called_kwargs = apply_async.call_args
        assert called_kwargs["kwargs"] == {"is_start": False}
        assert called_kwargs["countdown"] == 3

    def test_weight_accepted_and_alters_drain_order(self, vtrr, monkeypatch):
        ran = []

        @vtrr.task
        def work(self, tag):
            ran.append(tag)

        monkeypatch.setattr(vtrr, "_schedule_workers", lambda *a, **k: None)
        # a1 weight=3 -> a2 lands at vt=4; b1/b2/b3 land at vt=1/2/3,
        # so all three B tasks drain before A's second task.
        work.queue(
            partition_key="A",
            tasks=[
                {"id": "a1", "weight": 3, "kwargs": {"tag": "A1"}},
                {"id": "a2", "weight": 1, "kwargs": {"tag": "A2"}},
            ],
        )
        work.queue(
            partition_key="B",
            tasks=[
                {"id": "b1", "kwargs": {"tag": "B1"}},
                {"id": "b2", "kwargs": {"tag": "B2"}},
                {"id": "b3", "kwargs": {"tag": "B3"}},
            ],
        )
        kick(work)
        assert ran == ["A1", "B1", "B2", "B3", "A2"]

    def test_default_weight_one_preserves_turn_based_order(self, vtrr, monkeypatch):
        ran = []

        @vtrr.task
        def work(self, tag):
            ran.append(tag)

        monkeypatch.setattr(vtrr, "_schedule_workers", lambda *a, **k: None)
        # No weight specified — defaults to 1, same as standard fair turn-based order.
        work.queue(
            partition_key="A",
            tasks=[
                {"id": "a1", "kwargs": {"tag": "A1"}},
                {"id": "a2", "kwargs": {"tag": "A2"}},
            ],
        )
        work.queue(partition_key="B", tasks=[{"id": "b1", "kwargs": {"tag": "B1"}}])
        kick(work)
        assert ran == ["A1", "B1", "A2"]

    def test_reschedule_survives_task_exception(self, vtrr, monkeypatch):
        ran = []

        @vtrr.task
        def work(self, tag):
            ran.append(tag)
            if tag == "boom":
                raise RuntimeError("kaboom")

        monkeypatch.setattr(vtrr, "_schedule_workers", lambda *a, **k: None)
        work.queue(
            partition_key="A",
            tasks=[
                {"id": "t1", "kwargs": {"tag": "boom"}},
                {"id": "t2", "kwargs": {"tag": "ok"}},
            ],
        )
        try:
            kick(work)
        except RuntimeError:
            pass
        # The finally-clause schedules the next dequeue even though the first
        # task raised, so the second task still runs.
        assert ran == ["boom", "ok"]

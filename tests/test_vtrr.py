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
    """Bound dequeue callable — returns [task_id, payload] (or []), dropping lease details."""
    return lambda: vtrr._dequeue_script(
        keys=vtrr._dequeue_keys,
        args=[vtrr._lease_seconds, "tok"],
    )[:2]


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

    def test_drain_resets_all_state(
        self, vtrr, redis_client, enqueue_script, dequeue_script
    ):
        enqueue_script(make_argv("t", [("A", "a1", [], {})]))
        dequeue_script()
        assert redis_client.get(vtrr._current_vt_key) == b"0"
        assert redis_client.exists(vtrr._task_key) == 0
        assert redis_client.exists(vtrr._partitions_vt_key) == 0

    def test_dequeue_empty_queue_returns_empty(self, dequeue_script):
        assert dequeue_script() == []

    def test_weight_advances_partition_vt_by_weight(
        self, vtrr, redis_client, enqueue_script
    ):
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


def lease(vtrr):
    """Dequeue via the real path; returns (task_id, token, timeouts)."""
    _, _, _, task_id, token, timeouts = vtrr._dequeue()
    return task_id, token, timeouts


def expire(vtrr, task_id):
    """Make a lease expired by moving its in-flight score into the past (no sleeping)."""
    vtrr._redis.zadd(vtrr._inflight_key, {task_id: 1}, xx=True)


class TestLeases:
    def test_dequeue_puts_item_in_flight_and_ack_removes_it(
        self, vtrr, redis_client, enqueue_script
    ):
        """Dequeue leases the item (in flight, timeouts 0, token stored); acking
        with that token deletes all of its lease state."""
        enqueue_script(make_argv("t", [("A", "a1", [], {})]))
        task_id, token, timeouts = lease(vtrr)
        assert (task_id, timeouts) == ("a1", 0)
        assert redis_client.zscore(vtrr._inflight_key, "a1") > 0
        assert redis_client.hget(vtrr._lease_key, "a1") == token.encode()

        assert vtrr.ack("a1", token) is True
        for key in (
            vtrr._inflight_key,
            vtrr._inflight_task_key,
            vtrr._lease_key,
            vtrr._timeouts_key,
            vtrr._meta_key,
        ):
            assert redis_client.exists(key) == 0

    def test_ack_with_wrong_or_stale_token_changes_nothing(
        self, vtrr, redis_client, enqueue_script
    ):
        """Lease, expire, then re-lease under a new token; acks with the old, a
        bogus, or an unknown id's token return False and leave the new lease intact."""
        enqueue_script(make_argv("t", [("A", "a1", [], {})]))
        _, old_token, _ = lease(vtrr)
        expire(vtrr, "a1")
        _, new_token, timeouts = lease(vtrr)  # reclaimed and re-granted
        assert timeouts == 1 and new_token != old_token

        before = redis_client.zscore(vtrr._inflight_key, "a1")
        assert vtrr.ack("a1", old_token) is False  # stale holder
        assert vtrr.ack("a1", "nope") is False
        assert vtrr.ack("missing", old_token) is False
        assert redis_client.zscore(vtrr._inflight_key, "a1") == before
        assert redis_client.hget(vtrr._lease_key, "a1") == new_token.encode()
        assert redis_client.hget(vtrr._timeouts_key, "a1") == b"1"

    def test_lease_expiry_uses_redis_time_plus_lease_seconds(
        self, vtrr, redis_client, enqueue_script
    ):
        """Lease an item and check its in-flight score is Redis TIME plus
        lease_seconds (within 5s)."""
        enqueue_script(make_argv("t", [("A", "a1", [], {})]))
        sec, usec = redis_client.time()
        now = sec + usec / 1e6
        lease(vtrr)
        assert (
            abs(
                redis_client.zscore(vtrr._inflight_key, "a1")
                - (now + vtrr._lease_seconds)
            )
            < 5
        )

    def test_expired_lease_is_reclaimed_by_dequeue_and_rescored(
        self, vtrr, redis_client, enqueue_script
    ):
        """Expire a1's lease; the next dequeue reclaims it, rescores it like a fresh
        enqueue (behind a2), and it is handed out again later."""
        enqueue_script(
            make_argv(
                "t", [("A", "a1", [], {}), ("A", "a2", [], {}), ("B", "b1", [], {})]
            )
        )
        assert lease(vtrr)[0] == "a1"  # vt 1
        assert lease(vtrr)[0] == "b1"  # vt 1; current_vt now 1
        expire(vtrr, "a1")

        # A fresh enqueue for A would be scored max(partition counter, current_vt + 1)
        partition_vt = float(redis_client.hget(vtrr._partitions_vt_key, "A"))
        current_vt = float(redis_client.get(vtrr._current_vt_key))
        expected = max(partition_vt, current_vt + 1)

        # this dequeue reclaims a1 first, but a2 (vt 2) is still ahead of it
        assert lease(vtrr)[0] == "a2"
        assert redis_client.zscore(vtrr._queue_key, "a1") == expected
        assert redis_client.zscore(vtrr._inflight_key, "a1") is None
        assert lease(vtrr)[0] == "a1"  # redelivered with its original payload

    def test_timeouts_count_reclaims_and_persist(
        self, vtrr, redis_client, enqueue_script
    ):
        """Timeouts is 0 on the first lease and +1 for each reclaim (by dequeue or
        reclaim()); it is never reset."""
        enqueue_script(make_argv("t", [("A", "a1", [], {})]))
        assert lease(vtrr)[2] == 0  # first dequeue is not a timeout
        expire(vtrr, "a1")
        assert lease(vtrr)[2] == 1  # one lease expired and was reclaimed
        expire(vtrr, "a1")
        assert vtrr.reclaim() == ["a1"]
        assert redis_client.hget(vtrr._timeouts_key, "a1") == b"2"
        assert lease(vtrr)[2] == 2

    def test_standalone_reclaim_only_takes_expired(
        self, vtrr, redis_client, enqueue_script
    ):
        """With a1 expired and b1 still live, reclaim() requeues only a1; a second
        call finds nothing."""
        enqueue_script(make_argv("t", [("A", "a1", [], {}), ("B", "b1", [], {})]))
        lease(vtrr)
        lease(vtrr)
        expire(vtrr, "a1")
        assert vtrr.reclaim() == ["a1"]
        assert redis_client.zscore(vtrr._queue_key, "a1") is not None
        assert (
            redis_client.zscore(vtrr._inflight_key, "b1") is not None
        )  # unexpired: untouched
        assert vtrr.reclaim() == []

    def test_wrapper_acks_after_task_even_on_failure(self, vtrr, monkeypatch):
        """A task that raises still has its lease acked by the wrapper's finally, so
        a Python error does not cause redelivery."""

        @vtrr.task
        def work(self):
            raise RuntimeError("boom")

        monkeypatch.setattr(vtrr, "_schedule_workers", lambda *a, **k: None)
        work.queue(partition_key="A", tasks=[{"id": "t1"}])
        with pytest.raises(RuntimeError):
            kick(work)
        assert vtrr._redis.zcard(vtrr._inflight_key) == 0  # acked in finally

    def test_timeouts_is_visible_to_task(self, vtrr, monkeypatch):
        """A task run from a first dequeue sees self.request.vtrr_timeouts == 0."""
        seen = []

        @vtrr.task
        def work(self):
            seen.append(self.request.vtrr_timeouts)

        monkeypatch.setattr(vtrr, "_schedule_workers", lambda *a, **k: None)
        work.queue(partition_key="A", tasks=[{"id": "t1"}])
        kick(work)
        assert seen == [0]


class TestRetryReleasesLease:
    def test_renew_extends_expired_but_unreclaimed_lease(
        self, vtrr, redis_client, enqueue_script
    ):
        """Expire a lease without reclaiming it; renewing with its token succeeds,
        pushes the expiry to a real future time, and reclaim() finds nothing."""
        enqueue_script(make_argv("t", [("A", "a1", [], {})]))
        _, token, _ = lease(vtrr)
        expire(vtrr, "a1")
        assert vtrr._renew_lease("a1", token) is True
        assert redis_client.zscore(vtrr._inflight_key, "a1") > 1e9
        assert vtrr.reclaim() == []

    def test_renew_fails_after_reclaim_regrant(self, vtrr, enqueue_script):
        """After expiry, a second dequeue reclaims and re-leases the item under a
        new token; renewing with the old token returns False."""
        enqueue_script(make_argv("t", [("A", "a1", [], {})]))
        _, old_token, _ = lease(vtrr)
        expire(vtrr, "a1")
        lease(vtrr)  # reclaimed and re-granted with a new token
        assert vtrr._renew_lease("a1", old_token) is False

    def test_retry_run_holds_lease_and_acks_at_end(self, vtrr, monkeypatch):
        """A task that calls self.retry() keeps its lease through the retry (1 in
        flight on both runs) and acks only after the final run."""
        in_flight = []

        @vtrr.task(max_retries=2)
        def work(self):
            in_flight.append(vtrr._redis.zcard(vtrr._inflight_key))
            if len(in_flight) == 1:
                raise self.retry(countdown=0)

        monkeypatch.setattr(vtrr, "_schedule_workers", lambda *a, **k: None)
        work.queue(partition_key="A", tasks=[{"id": "t1"}])
        # eager mode runs the retry after the first run returns; don't let Retry propagate
        work._celery_task.apply(kwargs={"is_start": True}, throw=False)
        assert in_flight == [1, 1]  # leased during the original run and the retry
        assert vtrr._redis.zcard(vtrr._inflight_key) == 0  # acked once, at the end

    def test_retry_dropped_when_lease_was_lost(self, vtrr):
        """A retry run carrying a stale lease token is dropped without running the
        task."""
        ran = []

        @vtrr.task
        def work(self):
            ran.append(1)

        payload = {
            "task_name": work.name,
            "args": [],
            "kwargs": {},
            "task_id": "t1",
            "token": "stale",
        }
        work._celery_task.apply(kwargs={"task_payload": payload})
        assert ran == []


class TestReclaimAndDispatch:
    def test_reclaims_expired_lease_and_schedules_a_trigger(
        self, vtrr, enqueue_script, monkeypatch
    ):
        """With a1's lease expired, reclaim_and_dispatch() returns ['a1'] and
        schedules one trigger using the task's Celery task."""

        @vtrr.task
        def work(self):
            pass

        enqueue_script(make_argv(work.name, [("A", "a1", [], {})]))
        lease(vtrr)
        expire(vtrr, "a1")
        scheduled = []
        monkeypatch.setattr(
            vtrr, "_schedule_workers", lambda n, t: scheduled.append((n, t))
        )

        assert vtrr.reclaim_and_dispatch() == ["a1"]
        assert scheduled == [(1, work._celery_task)]

    def test_tops_up_triggers_for_an_idle_queue_with_nothing_reclaimed(
        self, vtrr, enqueue_script, monkeypatch
    ):
        """Two items waiting and nothing expired: it still schedules 2 triggers,
        covering triggers lost while the queue sat idle."""

        # e.g. every trigger was lost: items are waiting but no worker will ever wake up
        @vtrr.task
        def work(self):
            pass

        enqueue_script(make_argv(work.name, [("A", "a1", [], {}), ("A", "a2", [], {})]))
        scheduled = []
        monkeypatch.setattr(vtrr, "_schedule_workers", lambda n, t: scheduled.append(n))

        assert vtrr.reclaim_and_dispatch() == []
        assert scheduled == [2]

    def test_unexpired_lease_is_left_alone(self, vtrr, enqueue_script):
        """A live lease is not reclaimed: it returns [] and the item stays in
        flight."""

        @vtrr.task
        def work(self):
            pass

        enqueue_script(make_argv(work.name, [("A", "a1", [], {})]))
        lease(vtrr)
        assert vtrr.reclaim_and_dispatch() == []
        assert vtrr._redis.zscore(vtrr._inflight_key, "a1") is not None


class TestReclaimSchedule:
    def test_disabled_by_default(self, vtrr, celery_app):
        """Without reclaim_interval there is no beat entry and no reclaim task
        registered with Celery."""
        assert vtrr.reclaim_beat_schedule() == {}
        assert not celery_app.conf.beat_schedule
        assert "vtrr_queue.reclaim.test" not in celery_app.tasks

    def test_enabled_registers_task_and_beat_entry(self, redis_client, celery_app):
        """With reclaim_interval and reclaim_queue, the beat entry is merged
        alongside existing entries and the reclaim task is registered."""
        celery_app.conf.beat_schedule = {"existing": {"task": "x", "schedule": 1.0}}
        q = VTRRQueue(
            redis_client=redis_client,
            celery_app=celery_app,
            name="test",
            reclaim_interval=600,
            reclaim_queue="other",
        )
        entry = celery_app.conf.beat_schedule["vtrr_queue.reclaim.test"]
        assert entry == {
            "task": "vtrr_queue.reclaim.test",
            "schedule": 600.0,
            "options": {"queue": "other"},
        }
        assert "existing" in celery_app.conf.beat_schedule  # merged, not replaced
        assert q.reclaim_beat_schedule() == {"vtrr_queue.reclaim.test": entry}
        assert "vtrr_queue.reclaim.test" in celery_app.tasks

    def test_queue_option_omitted_when_unset(self, redis_client, celery_app):
        """Without reclaim_queue the beat entry has no 'options', so Celery uses its
        default queue."""
        q = VTRRQueue(
            redis_client=redis_client,
            celery_app=celery_app,
            name="test",
            reclaim_interval=60,
        )
        assert "options" not in q.reclaim_beat_schedule()["vtrr_queue.reclaim.test"]

    def test_registered_celery_task_runs_reclaim_and_dispatch(
        self, redis_client, celery_app, monkeypatch
    ):
        """Running the registered reclaim Celery task calls reclaim_and_dispatch()
        once."""
        q = VTRRQueue(
            redis_client=redis_client,
            celery_app=celery_app,
            name="test",
            reclaim_interval=60,
        )
        called = []
        monkeypatch.setattr(q, "reclaim_and_dispatch", lambda: called.append(1))
        celery_app.tasks["vtrr_queue.reclaim.test"].apply()
        assert called == [1]

    @pytest.mark.parametrize("interval", [0, -5])
    def test_rejects_non_positive_interval(self, redis_client, celery_app, interval):
        """A zero or negative reclaim_interval raises ValueError at construction."""
        with pytest.raises(ValueError):
            VTRRQueue(
                redis_client=redis_client,
                celery_app=celery_app,
                name="test",
                reclaim_interval=interval,
            )

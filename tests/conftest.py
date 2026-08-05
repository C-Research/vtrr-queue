"""Shared fixtures for the VTRR queue test suite."""

import fakeredis
import pytest
from celery import Celery

from vtrr_queue.queue import VTRRQueue


@pytest.fixture
def celery_app():
    app = Celery("vtrr_tests")
    app.conf.update(
        task_always_eager=True,  # run tasks synchronously, in-process
        task_eager_propagates=True,  # surface task exceptions by default
        broker_url="memory://",
        result_backend="cache+memory://",
    )
    return app


@pytest.fixture
def redis_client():
    return fakeredis.FakeStrictRedis(decode_responses=False, protocol=2)


@pytest.fixture
def vtrr(redis_client, celery_app):
    q = VTRRQueue(redis_client=redis_client, celery_app=celery_app, name="test")
    q._broker = fakeredis.FakeStrictRedis(decode_responses=False)
    return q

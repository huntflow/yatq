import json

import pytest

from yatq.dto import Task
from yatq.worker.factory.simple import SimpleJobFactory
from yatq.worker.job.simple import SimpleJob


def test_simple_job_creation():
    class JobA(SimpleJob):
        async def run(self, **kwargs):
            pass

    class JobB(SimpleJob):
        async def run(self, **kwargs):
            pass

    handlers = {
        "job_a": JobA,
        "job_b": JobB,
    }

    task_a = Task(
        "test",
        timeout=0,
        encoded_data=json.dumps({"kwargs": {"test": 1}, "name": "job_a"}),
    )
    task_b = Task(
        "test",
        timeout=0,
        encoded_data=json.dumps({"kwargs": {"test": 1}, "name": "job_b"}),
    )

    factory = SimpleJobFactory(handlers=handlers)

    job_a = factory.create_job(task_a)
    assert isinstance(job_a, JobA)

    job_b = factory.create_job(task_b)
    assert isinstance(job_b, JobB)


def test_missing_key_job_creation():
    task = Task(
        "test",
        timeout=0,
        encoded_data=json.dumps({"kwargs": {"test": 1}, "name": "job"}),
    )
    factory = SimpleJobFactory(handlers={})

    with pytest.raises(KeyError):
        factory.create_job(task)


def test_missing_value_job_creation():
    task = Task(
        "test",
        timeout=0,
        encoded_data=json.dumps({}),
    )
    factory = SimpleJobFactory(handlers={})

    with pytest.raises(ValueError):
        factory.create_job(task)

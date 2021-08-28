import json
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest

from yatq.dto import Task
from yatq.worker.job.simple import SimpleJob

# NOTE: Rewrite using AsyncMock after dropping py37 support


def test_simple_job_abstract():
    task = Task("test", 0)

    with pytest.raises(TypeError):
        SimpleJob(task)


def test_simple_job_abstract_incomplete_implementation():
    task = Task("test", 0)

    class Job(SimpleJob):
        async def post_process(self):
            pass

    with pytest.raises(TypeError):
        Job(task)


def test_simple_job_abstract_complete_implementation():
    task = Task("test", timeout=0, encoded_data=json.dumps({"kwargs": {"test": 1}}))

    class Job(SimpleJob):
        async def run(self):
            pass

    Job(task)


@pytest.mark.asyncio
async def test_job_simple_call_partial_implementation():
    task = Task("test", timeout=0, encoded_data=json.dumps({"kwargs": {"test": 1}}))

    pre_run = MagicMock()
    run = MagicMock()
    post_run = MagicMock()

    class Job(SimpleJob):
        @asynccontextmanager
        async def run_context(self):
            pre_run()
            try:
                yield
            finally:
                post_run()

        async def run(self, **kwargs) -> None:
            run(**kwargs)

    job = Job(task)
    await job.process()

    pre_run.assert_called_once()
    run.assert_called_once_with(test=1)
    post_run.assert_called_once()

    await job.do_post_process()


@pytest.mark.asyncio
async def test_job_simple_call_full_implementation():
    task = Task("test", timeout=0, encoded_data=json.dumps({"kwargs": {"test": 1}}))

    pre_run = MagicMock()
    run = MagicMock()
    post_run = MagicMock()
    post_process = MagicMock()

    class Job(SimpleJob):
        @asynccontextmanager
        async def run_context(self):
            pre_run()
            try:
                yield
            finally:
                post_run()

        async def run(self, **kwargs) -> None:
            run(**kwargs)

        async def post_process(self, **kwargs) -> None:
            post_process(**kwargs)

    job = Job(task)
    await job.process()

    pre_run.assert_called_once()
    run.assert_called_once_with(test=1)
    post_run.assert_called_once()
    post_process.assert_not_called()

    await job.do_post_process()
    post_process.assert_called_once_with(test=1)


@pytest.mark.asyncio
async def test_job_simple_call_non_json_result():
    task = Task("test", timeout=0, encoded_data=json.dumps({"kwargs": {"test": 1}}))

    class Job(SimpleJob):
        async def run(self, **kwargs) -> None:
            return object()

    job = Job(task)
    await job.process()
    await job.do_post_process()

    result_string = json.loads(job.task.result)
    assert result_string.startswith("<object object")

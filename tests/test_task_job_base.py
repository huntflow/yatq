from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest

from yatq.dto import Task
from yatq.worker.job.base import BaseJob

# NOTE: Rewrite using AsyncMock after dropping py37 support


def test_base_job_abstract():
    task = Task("test", 0)

    with pytest.raises(TypeError):
        BaseJob(task)


def test_base_job_abstract_incomplete_implementation():
    task = Task("test", 0)

    class Job(BaseJob):
        async def post_process(self):
            pass

    with pytest.raises(TypeError):
        Job(task)


def test_base_job_abstract_complete_implementation():
    task = Task("test", 0)

    class Job(BaseJob):
        async def run(self):
            pass

    Job(task)


@pytest.mark.asyncio
async def test_base_job_run():
    task = Task("test", 0)

    class Job(BaseJob):
        async def run(self):
            pass

    job = Job(task)

    await job.process()
    await job.do_post_process()


@pytest.mark.asyncio
async def test_job_post_process():
    task = Task({"test": "test"}, 0)

    pre_run = MagicMock()
    run = MagicMock()
    post_run = MagicMock()
    post_process = MagicMock()

    class Job(BaseJob):
        @asynccontextmanager
        async def run_context(self):
            pre_run()
            try:
                yield
            finally:
                post_run()

        async def run(self) -> None:
            run()

        async def post_process(self) -> None:
            post_process()

    job = Job(task)
    await job.process()

    pre_run.assert_called_once()
    run.assert_called_once()
    post_run.assert_called_once()
    post_process.assert_not_called()

    await job.post_process()
    post_process.assert_called_once()


@pytest.mark.asyncio
async def test_job_pre_exception():
    task = Task("test", 0)

    run = MagicMock()
    post_run = MagicMock()

    class Job(BaseJob):
        @asynccontextmanager
        async def run_context(self):
            raise ValueError(1)
            try:
                yield
            finally:
                post_run()

        async def run(self) -> None:
            run()

        async def post_process(self) -> None:
            post_process()

    job = Job(task)

    with pytest.raises(ValueError):
        await job.process()

    run.assert_not_called()
    post_run.assert_not_called()


@pytest.mark.asyncio
async def test_job_run_exception():
    task = Task("test", 0)

    pre_run = MagicMock()
    post_run = MagicMock()

    class Job(BaseJob):
        @asynccontextmanager
        async def run_context(self):
            pre_run()
            try:
                yield
            finally:
                post_run()

        async def run(self) -> None:
            raise ValueError(1)

        async def post_process(self) -> None:
            post_process()

    job = Job(task)

    with pytest.raises(ValueError):
        await job.process()

    pre_run.assert_called_once()
    post_run.assert_called_once()


@pytest.mark.asyncio
async def test_job_post_exception():
    task = Task("test", 0)

    pre_run = MagicMock()
    run = MagicMock()

    class Job(BaseJob):
        @asynccontextmanager
        async def run_context(self):
            pre_run()
            try:
                yield
            finally:
                raise ValueError(1)

        async def run(self) -> None:
            run()

        async def post_process(self) -> None:
            post_process()

    job = Job(task)

    with pytest.raises(ValueError):
        await job.process()

    pre_run.assert_called_once()
    run.assert_called_once()


@pytest.mark.asyncio
async def test_job_duration_processing(freezer):
    task = Task("test", {"test": "test"}, 0)

    class Job(BaseJob):
        @asynccontextmanager
        async def run_context(self):
            freezer.tick(1)
            try:
                yield
            finally:
                freezer.tick(1)

        async def run(self) -> None:
            freezer.tick(3)

        async def post_process(self) -> None:
            freezer.tick(5)

    job = Job(task)

    assert job.process_duration == 0.0
    assert job.post_process_duration == 0.0

    await job.process()
    await job.do_post_process()

    assert job.process_duration == 3.0
    assert job.post_process_duration == 5.0

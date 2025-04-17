import asyncio
from typing import Any
from unittest.mock import Mock

import pytest

from yatq.enums import RetryPolicy, TaskState
from yatq.exceptions import RetryTask
from yatq.queue import Queue
from yatq.worker.factory.simple import SimpleJobFactory
from yatq.worker.job.simple import SimpleJob
from yatq.worker.runner import build_worker
from yatq.worker.worker_settings import WorkerSettings


@pytest.mark.asyncio
async def test_worker_start_stop(redis_connection, task_queue: Queue):
    event = asyncio.Event()

    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            event.set()

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task({"name": "job", "kwargs": {}})

    await asyncio.wait_for(event.wait(), timeout=1)
    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)

    assert not run_task.exception()
    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.COMPLETED
    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_start_stop_default_namespace(
    redis_connection, task_queue_default_namespace: Queue
):
    task_queue = task_queue_default_namespace
    event = asyncio.Event()

    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            event.set()

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task({"name": "job", "kwargs": {}})

    await asyncio.wait_for(event.wait(), timeout=1)
    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)

    assert not run_task.exception()
    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.COMPLETED
    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_start_stop_clean_exit(redis_connection, task_queue: Queue):
    event = asyncio.Event()

    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            event.set()

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task({"name": "job", "kwargs": {}})

    await asyncio.wait_for(event.wait(), timeout=1)
    await asyncio.wait_for(worker.completed_task.wait(), timeout=1)
    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)

    assert not run_task.exception()
    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.COMPLETED
    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_multiple_pending_tasks(redis_connection, task_queue: Queue):
    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            pass

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        max_jobs=1,
        queue_namespace=task_queue.namespace,
    )

    scheduled_task_1 = await task_queue.add_task({"name": "job", "kwargs": {}})
    scheduled_task_2 = await task_queue.add_task({"name": "job", "kwargs": {}})

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    await asyncio.wait_for(worker.got_task.wait(), timeout=1)
    assert worker.should_get_new_task is False

    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert not run_task.exception()

    task_1 = await task_queue.check_task(scheduled_task_1.id)
    assert task_1.state == TaskState.COMPLETED

    task_2 = await task_queue.check_task(scheduled_task_2.id)
    assert task_2.state == TaskState.QUEUED

    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_job_creation_failed(redis_connection, task_queue: Queue):
    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task({"name": "job", "kwargs": {}})
    await asyncio.wait_for(worker.got_task.wait(), timeout=1)

    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert not run_task.exception()

    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.FAILED
    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_job_coroutine_creation_failed(
    redis_connection, task_queue: Queue
):
    class Job(SimpleJob):
        async def process(self, required_arg) -> None:
            await super().process()

        async def run(self, **kwargs) -> Any:
            pass

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task({"name": "job", "kwargs": {}})
    await asyncio.wait_for(worker.got_task.wait(), timeout=1)

    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert not run_task.exception()

    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.FAILED
    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_job_run_failed(redis_connection, task_queue: Queue):
    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            raise ValueError

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task({"name": "job", "kwargs": {}})
    await asyncio.wait_for(worker.got_task.wait(), timeout=1)

    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert not run_task.exception()

    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.FAILED
    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_job_post_process_failed(redis_connection, task_queue: Queue):
    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            pass

        async def post_process(self, **kwargs) -> None:
            raise ValueError()

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task({"name": "job", "kwargs": {}})
    await asyncio.wait_for(worker.got_task.wait(), timeout=1)

    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert not run_task.exception()

    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.COMPLETED
    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_job_run_failed_requeued(redis_connection, task_queue: Queue):
    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            raise ValueError

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task(
        {"name": "job", "kwargs": {}}, retry_policy=RetryPolicy.LINEAR
    )
    await asyncio.wait_for(worker.got_task.wait(), timeout=1)

    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert not run_task.exception()

    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.REQUEUED
    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_job_run_failed_requeued_manually(
    redis_connection, task_queue: Queue
):
    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            raise RetryTask(force=True)

    scheduled_task = await task_queue.add_task(
        {"name": "job", "kwargs": {}},
        retry_policy=RetryPolicy.LINEAR,
        retry_limit=1,
    )
    task = await task_queue.check_task(scheduled_task.id)
    assert task.is_last_attempt is False

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)
    await asyncio.wait_for(worker.got_task.wait(), timeout=1)

    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert not run_task.exception()

    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.REQUEUED
    assert len(worker._job_handlers) == 0
    assert task.is_last_attempt is True


@pytest.mark.asyncio
async def test_worker_task_gravekeeper(freezer, redis_connection, task_queue: Queue):
    scheduled_task = await task_queue.add_task(
        {"name": "job", "kwargs": {}},
        task_timeout=0,
    )
    await task_queue.get_task()
    freezer.tick(10)
    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
        default_ttl=60,
    )
    await worker._call_gravekeeper()

    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.BURIED


@pytest.mark.asyncio
async def test_worker_on_task_process_exception(redis_connection, task_queue: Queue):
    exception_handler = Mock()

    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            raise ValueError

    async def on_task_process_exception(job, exc_info):
        exception_handler()

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
        on_task_process_exception=on_task_process_exception,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task({"name": "job", "kwargs": {}})
    await asyncio.wait_for(worker.got_task.wait(), timeout=1)

    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert not run_task.exception()

    exception_handler.assert_called_once()

    task = await task_queue.check_task(scheduled_task.id)
    assert task.state == TaskState.FAILED
    assert len(worker._job_handlers) == 0


@pytest.mark.asyncio
async def test_worker_on_task_process_exception_failure(
    redis_connection, task_queue: Queue
):
    class Job(SimpleJob):
        async def run(self, **kwargs) -> Any:
            raise ValueError

    async def on_task_process_exception(exc_info):
        raise Exception("FAIL")

    worker = build_worker(
        redis_connection,
        SimpleJobFactory,
        {"handlers": {"job": Job}},
        [task_queue.name],
        queue_namespace=task_queue.namespace,
        on_task_process_exception=on_task_process_exception,
    )

    run_coro = worker.run()
    run_task = asyncio.create_task(run_coro)
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduled_task = await task_queue.add_task({"name": "job", "kwargs": {}})
    await asyncio.wait_for(worker.got_task.wait(), timeout=1)

    await worker.stop()
    await asyncio.wait_for(run_task, timeout=1)
    assert not run_task.exception()

    task = await task_queue.check_task(scheduled_task.id)
    assert task
    assert task.state == TaskState.FAILED
    assert len(worker._job_handlers) == 0

from unittest.mock import MagicMock

import pytest

from yatq.dto import Task
from yatq.worker import BaseTaskRunner

# NOTE: Rewrite using AsyncMock after dropping py37 support


@pytest.mark.asyncio
async def test_base_runner():
    ctx = {}
    task = Task("test", {"test": "test"}, 0)

    runner = BaseTaskRunner(ctx, task)
    await runner.process()
    await runner.post_process()


@pytest.mark.asyncio
async def test_runner_post_process():
    ctx = {}
    task = Task("test", {"test": "test"}, 0)

    pre_run = MagicMock()
    run = MagicMock()
    post_run = MagicMock()
    post_process = MagicMock()

    class Runner(BaseTaskRunner):
        async def pre_run(self) -> None:
            pre_run()

        async def run(self) -> None:
            run()

        async def post_run(self) -> None:
            post_run()

        async def post_process(self) -> None:
            post_process()

    runner = Runner(ctx, task)
    await runner.process()

    pre_run.assert_called_once()
    run.assert_called_once()
    post_run.assert_called_once()
    post_process.assert_not_called()

    await runner.post_process()
    post_process.assert_called_once()


@pytest.mark.asyncio
async def test_runner_pre_exception():
    ctx = {}
    task = Task("test", {"test": "test"}, 0)

    run = MagicMock()
    post_run = MagicMock()

    class Runner(BaseTaskRunner):
        async def pre_run(self) -> None:
            raise ValueError(1)

        async def run(self) -> None:
            run()

        async def post_run(self) -> None:
            post_run()

    runner = Runner(ctx, task)

    with pytest.raises(ValueError):
        await runner.process()

    run.assert_not_called()
    post_run.assert_not_called()


@pytest.mark.asyncio
async def test_runner_run_exception():
    ctx = {}
    task = Task("test", {"test": "test"}, 0)

    pre_run = MagicMock()
    post_run = MagicMock()

    class Runner(BaseTaskRunner):
        async def pre_run(self) -> None:
            pre_run()

        async def run(self) -> None:
            raise ValueError(1)

        async def post_run(self) -> None:
            post_run()

    runner = Runner(ctx, task)

    with pytest.raises(ValueError):
        await runner.process()

    pre_run.assert_called_once()
    post_run.assert_called_once()


@pytest.mark.asyncio
async def test_runner_post_exception():
    ctx = {}
    task = Task("test", {"test": "test"}, 0)

    pre_run = MagicMock()
    run = MagicMock()

    class Runner(BaseTaskRunner):
        async def pre_run(self) -> None:
            pre_run()

        async def run(self) -> None:
            run()

        async def post_run(self) -> None:
            raise ValueError(1)

    runner = Runner(ctx, task)

    with pytest.raises(ValueError):
        await runner.process()

    pre_run.assert_called_once()
    run.assert_called_once()


@pytest.mark.asyncio
async def test_runner_duration_processing(freezer):
    ctx = {}
    task = Task("test", {"test": "test"}, 0)

    class Runner(BaseTaskRunner):
        async def pre_run(self) -> None:
            freezer.tick(1)

        async def run(self) -> None:
            freezer.tick(3)

        async def post_run(self) -> None:
            freezer.tick(1)

        async def post_process(self) -> None:
            freezer.tick(5)

    runner = Runner(ctx, task)

    assert runner.process_duration == 0.0
    assert runner.post_process_duration == 0.0

    await runner.process()
    await runner.do_post_process()

    assert runner.process_duration == 3.0
    assert runner.post_process_duration == 5.0

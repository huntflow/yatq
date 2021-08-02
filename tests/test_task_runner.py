from unittest.mock import MagicMock

import pytest

from yatq.dto import Task
from yatq.worker import BaseTaskRunner

# NOTE: Rewrite using AsyncMock after dropping py37 support


@pytest.mark.asyncio
async def test_base_runner():
    ctx = {}
    task = Task("test", 0)

    runner = BaseTaskRunner(ctx, task)
    await runner.process()


@pytest.mark.asyncio
async def test_runner_pre_exception():
    ctx = {}
    task = Task("test", 0)

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
    task = Task("test", 0)

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
    task = Task("test", 0)

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

import pytest

from yatq.dto import TaskState


@pytest.mark.asyncio
async def test_resurrect_task_pending(task_queue, queue_checker, queue_breaker):
    task_id_1 = await task_queue.add_task("test", {"test": "test"}, task_key="key")

    task = await task_queue.get_task()
    assert task.task.id == task_id_1

    await queue_breaker.drop_task_processing("key")

    task_id_2 = await task_queue.add_task("test", {"test": "test"}, task_key="key")

    task.task.state = TaskState.COMPLETED
    await task_queue.complete_task(task)

    await queue_checker.assert_state(task_id_1, TaskState.COMPLETED)
    await queue_checker.assert_state(task_id_2, TaskState.QUEUED)

    await queue_checker.assert_metric_added(2)


@pytest.mark.asyncio
async def test_resurrect_task_processing(task_queue, queue_breaker, queue_checker):
    task_id_1 = await task_queue.add_task("test", {"test": "test"}, task_key="key")

    task = await task_queue.get_task()
    assert task.task.id == task_id_1

    await queue_breaker.drop_task_processing("key")

    task_id_2 = await task_queue.add_task("test", {"test": "test"}, task_key="key")
    task_2 = await task_queue.get_task()

    task.task.state = TaskState.COMPLETED
    await task_queue.complete_task(task)

    await queue_checker.assert_state(task_id_1, TaskState.COMPLETED)
    await queue_checker.assert_state(task_id_2, TaskState.PROCESSING)

    task_2.task.state = TaskState.COMPLETED
    await task_queue.complete_task(task_2)

    await queue_checker.assert_state(task_id_1, TaskState.COMPLETED)
    await queue_checker.assert_state(task_id_2, TaskState.COMPLETED)

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_completed(1)
    await queue_checker.assert_metric_resurrected(1)


@pytest.mark.asyncio
async def test_resurrect_not_unique(task_queue, queue_checker):
    task_id_1 = await task_queue.add_task("test", {"test": "test"}, task_timeout=0)

    task = await task_queue.get_task()
    assert task.task.id == task_id_1

    buried = await task_queue.bury_tasks()
    assert buried == 1

    await queue_checker.assert_state(task_id_1, TaskState.BURIED)

    task.task.state = TaskState.COMPLETED
    await task_queue.complete_task(task)

    await queue_checker.assert_state(task_id_1, TaskState.COMPLETED)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_buried(1)
    await queue_checker.assert_metric_resurrected(1)

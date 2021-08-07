import pytest

from yatq.dto import TaskState


@pytest.mark.asyncio
async def test_resurrect_task_pending(task_queue, queue_checker, queue_breaker):
    task_1 = await task_queue.add_task({"test": "test"}, task_key="key")

    task = await task_queue.get_task()
    assert task.task.id == task_1.id

    await queue_breaker.drop_task_processing("key")

    task_2 = await task_queue.add_task({"test": "test"}, task_key="key")

    task.task.state = TaskState.COMPLETED
    await task_queue.complete_task(task)

    await queue_checker.assert_state(task_1.id, TaskState.COMPLETED)
    await queue_checker.assert_state(task_2.id, TaskState.QUEUED)

    await queue_checker.assert_metric_added(2)


@pytest.mark.asyncio
async def test_resurrect_task_processing(task_queue, queue_breaker, queue_checker):
    scheduled_task_1 = await task_queue.add_task({"test": "test"}, task_key="key")

    task = await task_queue.get_task()
    assert task.task.id == scheduled_task_1.id

    await queue_breaker.drop_task_processing("key")

    scheduled_task_2 = await task_queue.add_task({"test": "test"}, task_key="key")
    task_2 = await task_queue.get_task()

    task.task.state = TaskState.COMPLETED
    await task_queue.complete_task(task)

    await queue_checker.assert_state(scheduled_task_1.id, TaskState.COMPLETED)
    await queue_checker.assert_state(scheduled_task_2.id, TaskState.PROCESSING)

    task_2.task.state = TaskState.COMPLETED
    await task_queue.complete_task(task_2)

    await queue_checker.assert_state(scheduled_task_1.id, TaskState.COMPLETED)
    await queue_checker.assert_state(scheduled_task_2.id, TaskState.COMPLETED)

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_completed(1)
    await queue_checker.assert_metric_resurrected(1)


@pytest.mark.asyncio
async def test_resurrect_not_unique(task_queue, queue_checker):
    scheduled_task_1 = await task_queue.add_task({"test": "test"}, task_timeout=0)

    task = await task_queue.get_task()
    assert task.task.id == scheduled_task_1.id

    buried = await task_queue.bury_tasks()
    assert buried == 1

    await queue_checker.assert_state(scheduled_task_1.id, TaskState.BURIED)

    task.task.state = TaskState.COMPLETED
    await task_queue.complete_task(task)

    await queue_checker.assert_state(scheduled_task_1.id, TaskState.COMPLETED)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_buried(1)
    await queue_checker.assert_metric_resurrected(1)

import pytest

from yatq.dto import TaskWrapper
from yatq.enums import RetryPolicy, TaskState
from yatq.exceptions import RescheduleLimitReached, TaskAddException, TaskRetryForbidden
from yatq.queue import Queue


@pytest.mark.asyncio
async def test_task_scheduling(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"})

    assert isinstance(task_1.id, str)

    task_info = await task_queue.check_task(task_1.id)
    assert task_info.state == TaskState.QUEUED
    assert task_info.id == task_1.id
    assert task_info.data["key"] == "value"

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    await queue_checker.assert_metric_added(1)


@pytest.mark.asyncio
async def test_task_check(task_queue):
    task_1 = await task_queue.add_task({"key": "value"})

    task = await task_queue.check_task(task_1.id)
    assert task.data == {"key": "value"}


@pytest.mark.asyncio
async def test_task_missing_check(task_queue):
    task = await task_queue.check_task("task-id")

    assert task is None


@pytest.mark.asyncio
async def test_task_scheduling_multiple_tasks(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"})
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    task_2 = await task_queue.add_task({"key": "value"})
    assert isinstance(task_2.id, str)
    await queue_checker.assert_state(task_2.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(2)
    await queue_checker.assert_processing_count(0)

    await queue_checker.assert_metric_added(2)


@pytest.mark.asyncio
async def test_task_scheduling_multiple_tasks_unique_key(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"}, task_key="unique_key")
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    with pytest.raises(TaskAddException) as error:
        await task_queue.add_task(
            {"key": "value"}, task_key="unique_key", ignore_existing=False
        )
    assert error.value.state == "PENDING"

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    await queue_checker.assert_metric_added(1)


@pytest.mark.asyncio
async def test_task_scheduling_multiple_tasks_unique_key_ignore_existing(
    task_queue, queue_checker
):
    task_1 = await task_queue.add_task({"key": "value"}, task_key="unique_key")
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    task_2 = await task_queue.add_task(
        {"key": "value"}, task_key="unique_key", ignore_existing=True
    )
    assert task_1.id == task_2.id

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    await queue_checker.assert_metric_added(1)


@pytest.mark.asyncio
async def test_task_scheduling_multiple_tasks_unique_non_conflicting(
    task_queue, queue_checker
):
    task_1 = await task_queue.add_task({"key": "value"}, task_key="unique_key")
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    task_2 = await task_queue.add_task({"key": "value"}, task_key="another_unique_key")
    assert isinstance(task_2.id, str)
    await queue_checker.assert_state(task_2.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(2)
    await queue_checker.assert_processing_count(0)

    await queue_checker.assert_metric_added(2)


@pytest.mark.asyncio
async def test_task_receiving(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"})
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    wrapped_task = await task_queue.get_task()
    assert isinstance(wrapped_task, TaskWrapper)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)


@pytest.mark.asyncio
async def test_task_unique_processing(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"}, task_key="unique_key")
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    wrapped_task = await task_queue.get_task()
    assert isinstance(wrapped_task, TaskWrapper)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)

    with pytest.raises(TaskAddException) as error:
        await task_queue.add_task(
            {"key": "value"}, task_key="unique_key", ignore_existing=False
        )

    assert error.value.state == TaskState.PROCESSING

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)


@pytest.mark.asyncio
async def test_task_unique_processing_non_conflicting(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"}, task_key="unique_key")
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    wrapped_task = await task_queue.get_task()
    assert isinstance(wrapped_task, TaskWrapper)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)

    task_2 = await task_queue.add_task({"key": "value"}, task_key="another_unique_key")
    assert isinstance(task_2.id, str)
    await queue_checker.assert_state(task_2.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(1)

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_taken(1)


@pytest.mark.asyncio
async def test_task_completion(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"})
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    task = await task_queue.get_task()
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    task.task.state = TaskState.COMPLETED

    await task_queue.complete_task(task)
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_state(task_1.id, TaskState.COMPLETED)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_completed(1)


@pytest.mark.asyncio
async def test_task_completion_failed(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"})
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    task = await task_queue.get_task()
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    task.task.state = TaskState.FAILED

    await task_queue.complete_task(task)
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_state(task_1.id, TaskState.FAILED)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_completed(1)


@pytest.mark.asyncio
async def test_task_failure(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"})
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    task = await task_queue.get_task()
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    await task_queue.fail_task(task)
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_state(task_1.id, TaskState.FAILED)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_completed(1)


@pytest.mark.asyncio
async def test_task_retry_no_policy(task_queue, queue_checker):
    task_1 = await task_queue.add_task({"key": "value"})
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    task = await task_queue.get_task()
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    with pytest.raises(TaskRetryForbidden):
        await task_queue.auto_reschedule_task(task)

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_state(task_1.id, TaskState.FAILED)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_completed(1)


@pytest.mark.asyncio
async def test_task_retry_linear_policy(task_queue, queue_checker, freezer):
    task_1 = await task_queue.add_task(
        {"key": "value"}, retry_policy=RetryPolicy.LINEAR, retry_delay=2
    )
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    task = await task_queue.get_task()
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    await task_queue.auto_reschedule_task(task)
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_state(task_1.id, TaskState.REQUEUED)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_requeued(1)

    # Task is not immediately available
    task = await task_queue.get_task()
    assert task is None

    # Task is not available after 1 seconds
    freezer.tick(1)
    task = await task_queue.get_task()
    assert task is None

    # Task is available after 3 seconds
    freezer.tick(2)
    task = await task_queue.get_task()
    assert task is not None
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)
    await task_queue.auto_reschedule_task(task)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(2)
    await queue_checker.assert_metric_requeued(2)

    # Second retry - task is not available after 3 seconds
    freezer.tick(3)
    task = await task_queue.get_task()
    assert task is None

    # Task is available after 5 seconds
    freezer.tick(2)
    task = await task_queue.get_task()
    assert task is not None
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)
    await task_queue.auto_reschedule_task(task)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(3)
    await queue_checker.assert_metric_requeued(3)


@pytest.mark.asyncio
async def test_task_retry_exponential_policy(task_queue, queue_checker, freezer):
    task_1 = await task_queue.add_task(
        {"key": "value"}, retry_policy=RetryPolicy.EXPONENTIAL, retry_delay=4
    )
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    task = await task_queue.get_task()
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    await task_queue.auto_reschedule_task(task)
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_state(task_1.id, TaskState.REQUEUED)

    # Task is not immediately available
    task = await task_queue.get_task()
    assert task is None

    # Task is not available after 3 seconds
    freezer.tick(3)
    task = await task_queue.get_task()
    assert task is None

    # Task is available after 5 seconds
    freezer.tick(2)
    task = await task_queue.get_task()
    assert task is not None
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)
    await task_queue.auto_reschedule_task(task)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(2)
    await queue_checker.assert_metric_requeued(2)

    # Second retry - task is not available after 15 seconds
    freezer.tick(15)
    task = await task_queue.get_task()
    assert task is None

    # Task is available after 17 seconds
    freezer.tick(2)
    task = await task_queue.get_task()
    assert task is not None
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)
    await task_queue.auto_reschedule_task(task)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(3)
    await queue_checker.assert_metric_requeued(3)


@pytest.mark.asyncio
async def test_task_retry_forced(task_queue, queue_checker, freezer):
    task_1 = await task_queue.add_task(
        {"key": "value"}, retry_policy=RetryPolicy.EXPONENTIAL, retry_delay=4
    )
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    task = await task_queue.get_task()
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    await task_queue.auto_reschedule_task(task, force=True)
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_state(task_1.id, TaskState.REQUEUED)

    task = await task_queue.get_task()
    assert task is not None


@pytest.mark.asyncio
async def test_task_retry_limit(task_queue, queue_checker):
    retry_limit = 3
    task_1 = await task_queue.add_task(
        {"key": "value"},
        retry_policy=RetryPolicy.LINEAR,
        retry_limit=retry_limit,
        retry_delay=0,
    )
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    for _ in range(retry_limit):
        task = await task_queue.get_task()
        assert task.task.id == task_1.id

        await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

        await queue_checker.assert_pending_count(0)
        await queue_checker.assert_processing_count(1)

        await task_queue.auto_reschedule_task(task)
        await queue_checker.assert_pending_count(1)
        await queue_checker.assert_processing_count(0)
        await queue_checker.assert_state(task_1.id, TaskState.REQUEUED)

    task = await task_queue.get_task()
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    with pytest.raises(RescheduleLimitReached):
        await task_queue.auto_reschedule_task(task)

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_state(task_1.id, TaskState.FAILED)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(4)
    await queue_checker.assert_metric_requeued(3)
    await queue_checker.assert_metric_completed(1)


@pytest.mark.asyncio
async def test_task_retry_delay(task_queue, queue_checker, freezer):
    task_1 = await task_queue.add_task(
        {"key": "value"}, retry_policy=RetryPolicy.LINEAR, retry_limit=1, retry_delay=2
    )
    assert isinstance(task_1.id, str)
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)

    task = await task_queue.get_task()
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    await task_queue.auto_reschedule_task(task)
    await queue_checker.assert_state(task_1.id, TaskState.REQUEUED)

    task = await task_queue.get_task()
    assert task is None

    freezer.tick(3.0)
    task = await task_queue.get_task()
    assert isinstance(task, TaskWrapper)
    await queue_checker.assert_state(task_1.id, TaskState.PROCESSING)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(2)
    await queue_checker.assert_metric_requeued(1)

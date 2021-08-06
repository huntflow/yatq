import pytest

from yatq.dto import TaskState


@pytest.mark.asyncio
async def test_task_gravekeeping(task_queue, queue_checker):
    await task_queue.add_task({"test": "test"}, task_timeout=0)
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    buried_count = await task_queue.bury_tasks()
    assert buried_count == 1
    await queue_checker.assert_state(task.task.id, TaskState.BURIED)

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(0)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_buried(1)


@pytest.mark.asyncio
async def test_task_gravekeeper_cleanup_pending(
    task_queue, queue_checker, queue_breaker, redis_connection
):
    await task_queue.add_task({"test": "test"}, task_key="key")
    await task_queue.add_task({"test": "test"}, task_key="key2")
    await queue_checker.assert_pending_count(2)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(2)

    await queue_breaker.drop_task_pending("key")
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(2)

    buried_count = await task_queue.bury_tasks()
    assert buried_count == 0

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    mapping_value = await redis_connection.hget(task_queue.mapping_key_name, "key2")
    assert mapping_value

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_task_gravekeeper_cleanup_processing(
    task_queue, queue_checker, queue_breaker, redis_connection
):
    await task_queue.add_task({"test": "test"}, task_key="key")
    await task_queue.add_task({"test": "test"}, task_key="key2")
    await task_queue.get_task()
    await task_queue.get_task()

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(2)
    await queue_checker.assert_mapping_len(2)

    await queue_breaker.drop_task_processing("key")
    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_mapping_len(2)

    buried_count = await task_queue.bury_tasks()
    assert buried_count == 0

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_mapping_len(1)

    mapping_value = await redis_connection.hget(task_queue.mapping_key_name, "key2")
    assert mapping_value

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_taken(2)
    await queue_checker.assert_metric_broken(1)

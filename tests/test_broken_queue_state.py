import pytest

from tests.conftest import zadd_single
from yatq.enums import RetryPolicy
from yatq.exceptions import RescheduledTaskMissing


@pytest.mark.asyncio
async def test_get_task_missing_data(task_queue, queue_checker, queue_breaker):
    task_id = await task_queue.add_task({"test": "test"})

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    await queue_breaker.drop_task_data(task_id.id)

    task = await task_queue.get_task()
    assert task is None

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(0)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_multuple_get_tasks_missing_data(
    task_queue, queue_breaker, queue_checker
):
    task_1 = await task_queue.add_task({"test": "test"})
    task_2 = await task_queue.add_task({"test": "test"})

    await queue_checker.assert_pending_count(2)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(2)

    await queue_breaker.drop_task_data(task_1.id)

    task = await task_queue.get_task()
    assert task is None

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    assert task.task.id == task_2.id

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_mapping_len(1)

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_get_task_missing_mapping(task_queue, queue_breaker, queue_checker):
    task_1 = await task_queue.add_task({"test": "test"})

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    await queue_breaker.drop_task_mapping(task_1.id)
    task = await task_queue.get_task()
    assert task is None

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(0)

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(0)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_multiple_get_tasks_missing_mapping(
    task_queue, queue_checker, queue_breaker
):
    task_1 = await task_queue.add_task({"test": "test"})
    task_2 = await task_queue.add_task({"test": "test"})

    await queue_checker.assert_pending_count(2)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(2)

    await queue_breaker.drop_task_mapping(task_1.id)
    task = await task_queue.get_task()
    assert task is None

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    assert task.task.id == task_2.id

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_mapping_len(1)

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_add_missing_mapping_key(task_queue, queue_breaker, queue_checker):
    task_1 = await task_queue.add_task({"test": "test"}, task_key="key")

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    await queue_breaker.drop_task_mapping("key")
    await queue_checker.assert_mapping_len(0)

    task_2 = await task_queue.add_task({"test": "test"}, task_key="key")

    assert task_1.id != task_2.id

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    assert task.task.id == task_2.id

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_add_existing_pending_missing_mapping_entry(
    task_queue, queue_checker, redis_connection
):
    await zadd_single(redis_connection, task_queue.pending_set_name, "key", 1)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(0)

    task_1 = await task_queue.add_task({"test": "test"}, task_key="key")
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    assert task.task.id == task_1.id

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_add_existing_processing_missing_mapping_entry(
    task_queue, queue_checker, redis_connection
):
    await zadd_single(redis_connection, task_queue.processing_set_name, "key", 1)

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_mapping_len(0)

    task_1 = await task_queue.add_task({"test": "test"}, task_key="key")
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    assert task.task.id == task_1.id

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_add_existing_processing_existing_pending_missing_mapping_entry(
    task_queue, queue_checker, redis_connection
):
    await zadd_single(redis_connection, task_queue.processing_set_name, "key", 1)
    await zadd_single(redis_connection, task_queue.pending_set_name, "key", 1)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_mapping_len(0)

    task_1 = await task_queue.add_task({"test": "test"}, task_key="key")
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    assert task.task.id == task_1.id

    await queue_checker.assert_metric_added(1)
    await queue_checker.assert_metric_taken(1)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_add_missing_data_pending(task_queue, queue_checker, queue_breaker):
    task_1 = await task_queue.add_task({"test": "test"}, task_key="key")
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    await queue_breaker.drop_task_data(task_1.id)

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task_2 = await task_queue.add_task({"test": "test"}, task_key="key")
    assert task_1.id != task_2.id

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_add_missing_data_processing(task_queue, queue_breaker, queue_checker):
    task_1 = await task_queue.add_task({"test": "test"}, task_key="key")
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    assert task.task.id == task_1.id
    await queue_breaker.drop_task_data(task_1.id)

    await queue_checker.assert_pending_count(0)
    await queue_checker.assert_processing_count(1)
    await queue_checker.assert_mapping_len(1)

    task_2 = await task_queue.add_task({"test": "test"}, task_key="key")
    assert task_1.id != task_2.id

    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    await queue_checker.assert_metric_added(2)
    await queue_checker.assert_metric_broken(1)


@pytest.mark.asyncio
async def test_reschedule_key_missing(task_queue, queue_breaker, queue_checker):
    task_1 = await task_queue.add_task(
        {"test": "test"}, task_key="key", retry_policy=RetryPolicy.LINEAR
    )
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    assert task.task.id == task_1.id
    await queue_breaker.drop_task_mapping("key")

    with pytest.raises(RescheduledTaskMissing):
        await task_queue.auto_reschedule_task(task)


@pytest.mark.asyncio
async def test_reschedule_key_changed(task_queue, queue_breaker, queue_checker):
    task_1 = await task_queue.add_task(
        {"test": "test"}, task_key="key", retry_policy=RetryPolicy.LINEAR
    )
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    task = await task_queue.get_task()
    assert task.task.id == task_1.id
    await queue_breaker.drop_task_mapping("key")

    task_2 = await task_queue.add_task({"test": "test2"}, task_key="key")
    await queue_checker.assert_pending_count(1)
    await queue_checker.assert_processing_count(0)
    await queue_checker.assert_mapping_len(1)

    assert task_2.id != task_1.id

    with pytest.raises(RescheduledTaskMissing):
        await task_queue.auto_reschedule_task(task)

    stored_task = await task_queue.check_task(task_2.id)
    assert stored_task.data["test"] == "test2"

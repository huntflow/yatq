import pytest

from yatq.enums import RetryPolicy, TaskState


@pytest.mark.asyncio
async def test_task_preserve_data_during_processing(task_queue, queue_checker):
    task_data = {
        "empty_list": [],
        "empty_object": {},
        "null": None,
        "digits": 1245,
        "string": "Привет",
        "subobject": {
            "a": 1,
            "b": 2,
        },
        "sublist": [[1, 2, []]],
    }
    task_1 = await task_queue.add_task(
        task_data=task_data,
        retry_policy=RetryPolicy.LINEAR,
        retry_limit=1,
        retry_delay=0,
    )
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)
    task = await task_queue.get_task()
    assert task.task.data == task_data

    await task_queue.auto_reschedule_task(task)
    task = await task_queue.get_task()
    assert task.task.data == task_data


@pytest.mark.asyncio
async def test_task_data_cleanup(task_queue, queue_checker):
    task_data = {
        "empty_list": [],
        "empty_object": {},
        "null": None,
        "digits": 1245,
        "string": "Привет",
        "subobject": {
            "a": 1,
            "b": 2,
        },
        "sublist": [[1, 2, []]],
    }
    task_1 = await task_queue.add_task(
        task_data=task_data,
        retry_policy=RetryPolicy.LINEAR,
        retry_limit=1,
        retry_delay=0,
        keep_completed_data=False,
    )
    await queue_checker.assert_state(task_1.id, TaskState.QUEUED)
    task = await task_queue.get_task()
    assert task.task.data == task_data

    task.task.state = TaskState.COMPLETED
    await task_queue.complete_task(task)

    completed_task = await task_queue.check_task(task.task.id)
    assert completed_task.data is None

from typing import Any
from uuid import uuid4

import aioredis
import pytest

from yatq.dto import TaskState
from yatq.queue import Queue

if aioredis.__version__ >= "2.0":

    async def create_redis_connection(redis_uri: str):
        return aioredis.from_url(redis_uri)

    async def zadd_single(client: aioredis.Redis, set_name: str, key: str, value: Any):
        await client.zadd(set_name, {key: value})


else:

    async def create_redis_connection(redis_uri: str):
        return await aioredis.create_redis(redis_uri)

    async def zadd_single(client: aioredis.Redis, set_name: str, key: str, value: Any):
        await client.zadd(set_name, value, key)


@pytest.fixture
def redis_uri(redis_proc) -> str:
    return f"redis://{redis_proc.host}:{redis_proc.port}"


@pytest.fixture
async def redis_connection(redis_uri):
    return await create_redis_connection(redis_uri)


@pytest.fixture
def task_queue(redis_connection) -> Queue:
    return Queue(client=redis_connection, name=str(uuid4()))


class QueueChecker:
    def __init__(self, queue: Queue):
        self.queue = queue

    async def assert_state(self, task_id: str, task_state: TaskState):
        task_info = await self.queue.check_task(task_id)

        assert task_info.state == task_state

    async def assert_pending_count(self, count: int):
        pending_count = await self.queue.get_pending_count()

        assert pending_count == count

    async def assert_processing_count(self, count: int):
        processing_count = await self.queue.get_processing_count()

        assert processing_count == count

    async def assert_mapping_len(self, count: int):
        mapping_len = await self.queue.client.hlen(self.queue.mapping_key_name)

        assert mapping_len == count

    async def assert_metric_added(self, value: int):
        stored_value = int(
            await self.queue.client.get(self.queue.metrics_added_key) or 0
        )

        assert stored_value == value

    async def assert_metric_taken(self, value: int):
        stored_value = int(
            await self.queue.client.get(self.queue.metrics_taken_key) or 0
        )

        assert stored_value == value

    async def assert_metric_requeued(self, value: int):
        stored_value = int(
            await self.queue.client.get(self.queue.metrics_requeued_key) or 0
        )

        assert stored_value == value

    async def assert_metric_completed(self, value: int):
        stored_value = int(
            await self.queue.client.get(self.queue.metrics_completed_key) or 0
        )

        assert stored_value == value

    async def assert_metric_resurrected(self, value: int):
        stored_value = int(
            await self.queue.client.get(self.queue.metrics_resurrected_key) or 0
        )

        assert stored_value == value

    async def assert_metric_buried(self, value: int):
        stored_value = int(
            await self.queue.client.get(self.queue.metrics_buried_key) or 0
        )

        assert stored_value == value

    async def assert_metric_broken(self, value: int):
        stored_value = int(
            await self.queue.client.get(self.queue.metrics_broken_key) or 0
        )

        assert stored_value == value


@pytest.fixture
def queue_checker(task_queue) -> QueueChecker:
    return QueueChecker(task_queue)


class QueueBreaker:
    def __init__(self, queue: Queue):
        self.queue = queue

    async def drop_task_data(self, task_id: str):
        await self.queue.client.delete(f"{self.queue.task_key_prefix}:{task_id}")

    async def drop_task_mapping(self, task_key: str):
        await self.queue.client.hdel(self.queue.mapping_key_name, task_key)

    async def drop_task_pending(self, task_key: str):
        await self.queue.client.zrem(self.queue.pending_set_name, task_key)

    async def drop_task_processing(self, task_key: str):
        await self.queue.client.zrem(self.queue.processing_set_name, task_key)


@pytest.fixture
def queue_breaker(task_queue) -> QueueBreaker:
    return QueueBreaker(task_queue)

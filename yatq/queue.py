import asyncio
import dataclasses
import datetime
import json
import logging
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, Optional, Union
from uuid import uuid4

from yatq.py_version import AIOREDIS_USE

if AIOREDIS_USE:
    from aioredis import Redis
else:
    from redis.asyncio import Redis  # type: ignore # pragma: no cover

from .defaults import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_QUEUE_NAMESPACE,
    DEFAULT_TASK_EXPIRATION,
    DEFAULT_TIMEOUT,
)
from .dto import QueueEvent, ScheduledTask, Task, TaskWrapper
from .enums import QueueAction, RetryPolicy, TaskState
from .exceptions import (
    RescheduledTaskMissing,
    RescheduleLimitReached,
    TaskAddException,
    TaskRescheduleException,
    TaskRetryForbidden,
)
from .function import LuaFunction
from .lua import (
    ADD_TEMPLATE,
    BURY_TEMPLATE,
    COMPLETE_TEMPLATE,
    DROP_TEMPLATE,
    GET_TEMPLATE,
    RESCHEDULE_TEMPLATE,
)

LOGGER = logging.getLogger(__name__)


def encode_task(task: Task) -> str:
    return json.dumps(dataclasses.asdict(task))


def decode_task(data: dict) -> Task:
    return Task(**data)


PATH_TYPE = Union[str, Path]


class Queue:
    def __init__(
        self,
        client: Redis,
        name: str = DEFAULT_QUEUE_NAME,
        namespace: str = DEFAULT_QUEUE_NAMESPACE,
        add_template: str = ADD_TEMPLATE,
        get_template: str = GET_TEMPLATE,
        complete_template: str = COMPLETE_TEMPLATE,
        reschedule_template: str = RESCHEDULE_TEMPLATE,
        bury_template: str = BURY_TEMPLATE,
        drop_template: str = DROP_TEMPLATE,
        logger: Optional[logging.Logger] = None,
        default_ttl: int = DEFAULT_TASK_EXPIRATION,
    ):
        assert ":" not in name, "Name should not contain ':'"
        assert ":" not in namespace, "Namespace should not contain ':'"

        self.client = client
        self.name = name
        self.namespace = namespace

        self.logger = logger or LOGGER

        self._key_prefix = f"{self.namespace}:{self.name}"
        self.event_channel_name = f"{self._key_prefix}:events"
        self.processing_set_name = f"{self._key_prefix}:processing"
        self.pending_set_name = f"{self._key_prefix}:pending"
        self.mapping_key_name = f"{self._key_prefix}:key_id_map"
        self.task_key_prefix = f"{self._key_prefix}:task"
        self.metrics_added_key = f"{self._key_prefix}:metrics:added"
        self.metrics_taken_key = f"{self._key_prefix}:metrics:taken"
        self.metrics_requeued_key = f"{self._key_prefix}:metrics:requeued"
        self.metrics_completed_key = f"{self._key_prefix}:metrics:completed"
        self.metrics_resurrected_key = f"{self._key_prefix}:metrics:resurrected"
        self.metrics_buried_key = f"{self._key_prefix}:metrics:buried"
        self.metrics_broken_key = f"{self._key_prefix}:metrics:broken"
        self.metrics_time_wait = f"{self._key_prefix}:metrics:time_wait"
        self.default_ttl = default_ttl
        self.environment = {
            "processing_key": self.processing_set_name,
            "pending_key": self.pending_set_name,
            "task_mapping_key": self.mapping_key_name,
            "event_channel": self.event_channel_name,
            "task_key_prefix": self.task_key_prefix,
            "metrics_added_key": self.metrics_added_key,
            "metrics_taken_key": self.metrics_taken_key,
            "metrics_requeued_key": self.metrics_requeued_key,
            "metrics_completed_key": self.metrics_completed_key,
            "metrics_resurrected_key": self.metrics_resurrected_key,
            "metrics_buried_key": self.metrics_buried_key,
            "metrics_broken_key": self.metrics_broken_key,
            "metrics_time_wait": self.metrics_time_wait,
            "default_timeout": DEFAULT_TIMEOUT,
            "default_task_expiration": self.default_ttl,
        }

        self._add_function = LuaFunction(add_template, self.environment)
        self._get_function = LuaFunction(get_template, self.environment)
        self._complete_function = LuaFunction(complete_template, self.environment)
        self._reschedule_function = LuaFunction(reschedule_template, self.environment)
        self._bury_function = LuaFunction(bury_template, self.environment)
        self._drop_function = LuaFunction(drop_template, self.environment)

    async def add_task(
        self,
        task_data: Dict[str, Any],
        task_key: Optional[str] = None,
        task_timeout: int = DEFAULT_TIMEOUT,
        retry_policy: RetryPolicy = RetryPolicy.NONE,
        retry_delay: int = 10,
        retry_limit: int = 3,
        ignore_existing: bool = True,
        ttl: Optional[int] = None,
        keep_completed_data: bool = True,
    ) -> ScheduledTask:
        task_id = str(uuid4())
        self.logger.debug("Task data to add: %s", task_data)

        if task_key is None:
            task_key = task_id

        if ttl is None:
            ttl = self.default_ttl

        task = Task(
            id=task_id,
            timeout=task_timeout,
            policy=retry_policy,
            delay=retry_delay,
            retry_limit=retry_limit,
            ttl=ttl,
            keep_completed_data=keep_completed_data,
        )
        task.data = task_data
        serialized_task = encode_task(task)
        self.logger.debug("Adding task: key = %s, task = %s", task_key, serialized_task)

        result: Dict[str, Any] = await self._add_function.call(
            self.client, task_key, task_id, serialized_task, time.time()
        )

        success: bool = result["success"]
        if success:
            return ScheduledTask(id=task_id, completed=asyncio.Event())

        if not ignore_existing:
            raise TaskAddException(
                state=result["state"],
                task_id=result["id"],
            )

        return ScheduledTask(id=result["id"], completed=asyncio.Event())

    async def get_task(self) -> Optional[TaskWrapper]:
        result = await self._get_function.call(self.client, time.time())
        self.logger.debug("Get task result: %s", result)

        if not result["success"]:
            error = result.get("error")
            if error:
                self.logger.warning("Error getting task: %s", error)
            return None

        task_key = result["key"]
        task_deadline = result["deadline"]
        data = result["data"]

        task = decode_task(data)
        return TaskWrapper(
            key=task_key,
            deadline=task_deadline,
            task=task,
            taken_at=datetime.datetime.now(),
        )

    async def complete_task(self, wrapped_task: TaskWrapper):
        assert wrapped_task.task.state in (
            TaskState.COMPLETED,
            TaskState.FAILED,
        ), "Task not in final state"

        if not wrapped_task.task.keep_completed_data:
            wrapped_task.task.data = None

        await self._complete_function.call(
            self.client,
            wrapped_task.key,
            wrapped_task.task.id,
            encode_task(wrapped_task.task),
            wrapped_task.task.ttl or DEFAULT_TASK_EXPIRATION,
        )

    async def fail_task(self, wrapped_task: TaskWrapper):
        wrapped_task.task.state = TaskState.FAILED
        await self.complete_task(wrapped_task)

    async def reschedule_task(self, wrapped_task: TaskWrapper, after: int):
        assert wrapped_task.task.state == TaskState.REQUEUED
        return await self._reschedule_function.call(
            self.client,
            wrapped_task.key,
            wrapped_task.task.id,
            encode_task(wrapped_task.task),
            after,
        )

    async def listen_events(self, queue: asyncio.Queue) -> None:
        receiver = self.client.pubsub()
        await receiver.subscribe(self.event_channel_name)
        try:
            async for message in receiver.listen():
                if not message or message["type"] != "message":
                    continue
                with suppress(asyncio.QueueFull):
                    event_type, task_id, task_key, *extra = (
                        message["data"].decode().split(" ", maxsplit=4)
                    )

                    event = QueueEvent(
                        queue=self.name,
                        action=QueueAction(event_type),
                        key=task_key if task_key != "NO_KEY" else None,
                        id=task_id if task_id != "NO_ID" else None,
                        message=extra[0] if extra else None,
                    )
                    queue.put_nowait(event)
        finally:
            await receiver.unsubscribe(self.event_channel_name)

    async def auto_reschedule_task(
        self, wrapped_task: TaskWrapper, force: bool = False
    ) -> int:
        task = wrapped_task.task
        task.retry_counter += 1

        if force:
            delay = 0
        else:
            exception: Optional[TaskRescheduleException] = None
            if task.policy == RetryPolicy.NONE:
                exception = TaskRetryForbidden()
            elif task.retry_counter > task.retry_limit:
                exception = RescheduleLimitReached()

            if exception:
                task.state = TaskState.FAILED
                await self.complete_task(wrapped_task)
                raise exception

            if task.policy == RetryPolicy.LINEAR:
                delay = task.delay * task.retry_counter
            elif task.policy == RetryPolicy.EXPONENTIAL:
                delay = task.delay**task.retry_counter
            else:
                delay = task.delay

        after_time = int(time.time()) + delay
        task.state = TaskState.REQUEUED

        result = await self.reschedule_task(wrapped_task, after=after_time)

        if result["success"]:
            return delay

        raise RescheduledTaskMissing()

    async def bury_tasks(self) -> int:
        result = await self._bury_function.call(self.client, time.time())
        return result["count"]

    async def drop_task(self, key: str) -> bool:
        result = await self._drop_function.call(self.client, key)
        return result["success"]

    async def check_task(self, task_id: str) -> Optional[Task]:
        task_data = await self.client.get(f"{self.task_key_prefix}:{task_id}")

        if not task_data:
            return None

        return decode_task(json.loads(task_data))

    async def get_processing_count(self) -> int:
        return await self.client.zcard(self.processing_set_name)

    async def get_pending_count(self) -> int:
        return await self.client.zcard(self.pending_set_name)

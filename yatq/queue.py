import asyncio
import dataclasses
import json
import logging
import time
from os.path import dirname
from pathlib import Path
from typing import Any, Dict, Optional, Union
from uuid import uuid4

from aioredis import Redis

from .defaults import (
    DEFAULT_QUEUE_NAME,
    DEFAULT_QUEUE_NAMESPACE,
    DEFAULT_TASK_EXPIRATION,
    DEFAULT_TIMEOUT,
)
from .dto import ScheduledTask, Task, TaskWrapper
from .enums import RetryPolicy, TaskState
from .exceptions import (
    RescheduledTaskMissing,
    RescheduleLimitReached,
    TaskAddException,
    TaskRescheduleException,
    TaskRetryForbidden,
)
from .function import LuaFunction

LOGGER = logging.getLogger(__name__)
DEFAULT_LUA_DIR = Path(dirname(__file__)) / "lua"


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
        add_src_path: PATH_TYPE = DEFAULT_LUA_DIR / "add_template.lua",
        get_src_path: PATH_TYPE = DEFAULT_LUA_DIR / "get_template.lua",
        complete_src_path: PATH_TYPE = DEFAULT_LUA_DIR / "complete_template.lua",
        reschedule_src_path: PATH_TYPE = DEFAULT_LUA_DIR / "reschedule_template.lua",
        bury_src_path: PATH_TYPE = DEFAULT_LUA_DIR / "bury_template.lua",
        logger: Optional[logging.Logger] = None,
    ):
        self.client = client
        self.name = name.replace(":", "_")
        self.namespace = namespace.replace(":", "_")

        self.logger = logger or LOGGER

        with open(add_src_path) as src:
            self._add_function = LuaFunction(src.read(), self.environment)

        with open(get_src_path) as src:
            self._get_function = LuaFunction(src.read(), self.environment)

        with open(complete_src_path) as src:
            self._complete_function = LuaFunction(src.read(), self.environment)

        with open(reschedule_src_path) as src:
            self._reschedule_function = LuaFunction(src.read(), self.environment)

        with open(bury_src_path) as src:
            self._bury_function = LuaFunction(src.read(), self.environment)

    @property
    def _key_prefix(self) -> str:
        return f"{self.namespace}:{self.name}"

    @property
    def event_channel_name(self) -> str:
        return f"{self._key_prefix}:events"

    @property
    def processing_set_name(self) -> str:
        return f"{self._key_prefix}:processing"

    @property
    def pending_set_name(self) -> str:
        return f"{self._key_prefix}:pending"

    @property
    def mapping_key_name(self) -> str:
        return f"{self._key_prefix}:key_id_map"

    @property
    def task_key_prefix(self) -> str:
        return f"{self._key_prefix}:task"

    @property
    def metrics_added_key(self) -> str:
        return f"{self._key_prefix}:metrics:added"

    @property
    def metrics_taken_key(self) -> str:
        return f"{self._key_prefix}:metrics:taken"

    @property
    def metrics_requeued_key(self) -> str:
        return f"{self._key_prefix}:metrics:requeued"

    @property
    def metrics_completed_key(self) -> str:
        return f"{self._key_prefix}:metrics:completed"

    @property
    def metrics_resurrected_key(self) -> str:
        return f"{self._key_prefix}:metrics:resurrected"

    @property
    def metrics_buried_key(self) -> str:
        return f"{self._key_prefix}:metrics:buried"

    @property
    def metrics_broken_key(self) -> str:
        return f"{self._key_prefix}:metrics:broken"

    @property
    def environment(self) -> Dict[str, Any]:
        return {
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
            "default_timeout": DEFAULT_TIMEOUT,
            "default_task_expiration": DEFAULT_TASK_EXPIRATION,
        }

    async def add_task(
        self,
        task_data: Dict[str, Any],
        task_key: Optional[str] = None,
        task_timeout: int = DEFAULT_TIMEOUT,
        retry_policy: RetryPolicy = RetryPolicy.NONE,
        retry_delay: int = 10,
        retry_limit: int = 3,
        ignore_existing: bool = True,
        ttl=DEFAULT_TASK_EXPIRATION,
        keep_completed_data=True,
    ) -> ScheduledTask:
        task_id = str(uuid4())
        self.logger.debug("Task data to add: %s", task_data)

        if task_key is None:
            task_key = task_id

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
            wrapped_task.task.ttl or 0,
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
            else:
                delay = task.delay ** task.retry_counter

        after_time = int(time.time()) + delay
        task.state = TaskState.REQUEUED

        result = await self.reschedule_task(wrapped_task, after=after_time)

        if result["success"]:
            return delay

        raise RescheduledTaskMissing()

    async def bury_tasks(self) -> int:
        result = await self._bury_function.call(self.client, time.time())
        return result["count"]

    async def check_task(self, task_id: str) -> Optional[Task]:
        task_data = await self.client.get(f"{self.task_key_prefix}:{task_id}")

        if not task_data:
            return None

        return decode_task(json.loads(task_data))

    async def get_processing_count(self) -> int:
        return await self.client.zcard(self.processing_set_name)

    async def get_pending_count(self) -> int:
        return await self.client.zcard(self.pending_set_name)

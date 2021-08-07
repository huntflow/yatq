import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, NamedTuple, Optional

from .defaults import DEFAULT_TASK_EXPIRATION
from .enums import QueueAction, RetryPolicy, TaskState


@dataclass
class QueueEvent:

    """
    Model of some event that happened in queue

    :param queue: Queue name
    :param action: Action type
    :param key: Related task key (if any)
    :param id: Related task id (if any)
    :param message: Optional log message, usually error
    """

    queue: str
    action: QueueAction

    key: Optional[str] = None
    id: Optional[str] = None

    message: Optional[str] = None


@dataclass
class Task:

    """
    Representation of task

    Note on keys and ids:
    Task ID represents unique task invocation, while task key is designed to be
    unique for task's *purpose*. For instance, multiple tasks are scheduled to reindex
    data of same database model. Thus, they key should be equal - for example, "reindex-1".
    If one task in schedule while another is already PENDING or PROCESSING, queue will reject it
    and instead return ID of existing task. On the other hand, if task execution does not overlap,
    each unique invocation data is stored, identified by task ID.

    Task timeout is used to calculate task's deadline after its transition to PROCESSING state.
    Calculated deadline is stored as task's rank in ZSET. Each consecutive rescheduling (in case task
    reschedule policy allow) resets deadline.

    :param id: task's unique id
    :param data: generic task data - a dict, for working within application
        (will not be sent to a lua script, see encoded_data)
    :param encoded_data: generic task data - json-encoded string.
        The cause (why it's needed to be encoded) is to prevent conversion of empty lists
        to js objects. https://github.com/redis/redis/issues/856
    :param timeout: task execution timeout in seconds
    :param ttl: How long to keep completed (or failed) task in seconds.
        0 or None means purge the task on completion
    :param keep_completed_data: if set to True, then the whole task data
        will be kept with completed or failed task. If it's False, then
        task data will be wiped out on task completion. The flag is needed to
        optimize storage usage. If there is no need to know task parameters for
        a completed task, then set the flag to False.
    :param result: execution result
    :param state: current task state
    :param policy: task rescheduling policy
    :param delay: task rescheduling delay (if policy is being used)
    :param retry_counter: number of times task was rescheduled
    :param retry_limit: maximum number of times task can be rescheduled (excluding worker-caused cancellations)
    """

    id: str
    timeout: int

    encoded_data: Optional[str] = None

    ttl: Optional[int] = DEFAULT_TASK_EXPIRATION
    keep_completed_data: bool = False

    result: Any = None
    state: TaskState = TaskState.QUEUED

    policy: RetryPolicy = RetryPolicy.NONE
    delay: int = 10

    retry_counter: int = 0
    retry_limit: int = 3

    @property
    def data(self) -> Optional[Dict]:
        if self.encoded_data is None:
            return None

        return json.loads(self.encoded_data)

    @data.setter
    def data(self, value: Optional[Dict]):
        if value is not None:
            self.encoded_data = json.dumps(value)
        else:
            self.encoded_data = None


class TaskWrapper(NamedTuple):

    """
    Representation of task inside queue

    :param key: unique key of task in queue
    :param task: task data
    :param deadline: task completion deadline (for task in PROCESSING state)
    """

    key: str
    task: Task
    deadline: Optional[datetime] = None


class ScheduledTask(NamedTuple):

    """
    Representation of newly created task

    :param id: Task id
    :param completed: Task completion event
    """

    id: str
    completed: asyncio.Event

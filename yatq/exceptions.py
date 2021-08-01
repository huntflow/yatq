from enum import Enum
from typing import Optional, Union


class BaseQueueException(Exception):
    def __init__(self, message: Optional[str] = None):
        super().__init__()
        self.message = message


class TaskAddException(BaseQueueException):
    def __init__(self, state: str, task_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = state
        self.id = task_id


class TaskRescheduleException(BaseQueueException):
    ...


class RescheduledTaskMissing(TaskRescheduleException):
    ...


class TaskRetryForbidden(TaskRescheduleException):
    ...


class RescheduleLimitReached(TaskRescheduleException):
    ...

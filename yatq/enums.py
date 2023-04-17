from enum import Enum


class RetryPolicy(str, Enum):

    """
    Options for task execution retrial

    NONE - no retry allowed
    LINEAR - delay between executions grows linearly
    EXPONENTIAL - delay between executions grows exponentially
    FIXED_INTERVAL - delay between executions is constant independent of retries
    """

    NONE = "NONE"
    LINEAR = "LINEAR"
    EXPONENTIAL = "EXPONENTIAL"
    FIXED_INTERVAL = "FIXED_INTERVAL"


class TaskState(str, Enum):

    """
    Enum with all possible task states in queue.

    QUEUED - freshly added task
    REQUEUED - task that was returned to queue by any reason
    PROCESSING - task that is currently being processed
    COMPLETED - task, which execution was completed without error
    FAILED - task, which execution failed and cannot be rescheduled
    BURIED - task, which state was not reported by worker before completion deadline
    """

    QUEUED = "QUEUED"
    REQUEUED = "REQUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    BURIED = "BURIED"


class QueueAction(str, Enum):

    """
    Types of events that can happen to tasks in queue. Used in queue events logging.

    ADDED - new task added to queue
    BROKEN - task data somehow lost consistency. More info in message
    BURIED - task was buried after reaching execution timeout
    BURY_ERROR - task burial failed. More info in message
    COMPLETED - task was completed successfully
    TAKEN - task was taken by worker and is currently being executed
    INVALID_STATE - task somehow got into invalid state. More info in message
    RESURRECTED - task got 'resurrected' - worker reported completion after deadline
    RESCHEDULE - task execution got rescheduled due to external reasons
    """

    ADDED = "ADDED"
    BROKEN = "BROKEN"
    BURIED = "BURIED"
    BURY_ERROR = "BURY_ERROR"
    COMPLETED = "COMPLETED"
    TAKEN = "TAKEN"
    INVALID_STATE = "INVALID_STATE"
    RESURRECTED = "RESURRECTED"
    RESCHEDULE = "RESCHEDULE"

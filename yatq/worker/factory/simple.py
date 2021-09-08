from typing import TYPE_CHECKING, Dict, Generic, Type, TypeVar, cast

from yatq.worker.factory.base import BaseJobFactory

if TYPE_CHECKING:  # pragma: no cover
    from yatq.dto import Task
    from yatq.worker.job.simple import SimpleJob


T_SimpleJob = TypeVar("T_SimpleJob", bound="SimpleJob")


class SimpleJobFactory(BaseJobFactory, Generic[T_SimpleJob]):

    """
    Simple job factory implementation. Job class is chosen from
    `handlers` mapping using `name` key from task data.
    """

    def __init__(
        self,
        handlers: Dict[str, Type[T_SimpleJob]],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.handlers = handlers

    def create_job(self, task: "Task") -> T_SimpleJob:
        task_data = task.data
        if not task_data:
            raise ValueError("Task data is not set")

        task_function = task_data["name"]
        handler_class = self.handlers[task_function]

        return handler_class(task)

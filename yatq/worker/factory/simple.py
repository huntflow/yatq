from typing import Dict, Type

from yatq.worker.factory.base import BaseJobFactory
from yatq.dto import Task
from yatq.worker.job.simple import SimpleJob


class SimpleJobFactory(BaseJobFactory[SimpleJob]):

    """
    Simple job factory implementation. Job class is chosen from
    `handlers` mapping using `name` key from task data.
    """

    def __init__(
        self,
        handlers: Dict[str, Type[SimpleJob]],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.handlers = handlers

    def create_job(self, task: Task) -> SimpleJob:
        task_data = task.data
        if not task_data:
            raise ValueError("Task data is not set")

        task_function = task_data["name"]
        handler_class = self.handlers[task_function]

        return handler_class(task)

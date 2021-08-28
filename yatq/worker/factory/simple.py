from typing import TYPE_CHECKING, Dict, Generic, Type, TypeVar

from yatq.worker.factory.base import BaseJobFactory
from yatq.worker.job.simple import SimpleJob

if TYPE_CHECKING:  # pragma: no cover
    from yatq.dto import Task


T_SimpleJobClass = TypeVar("T_SimpleJobClass", bound=SimpleJob)


class SimpleJobFactory(BaseJobFactory, Generic[T_SimpleJobClass]):
    def __init__(
        self,
        handlers: Dict[str, Type[T_SimpleJobClass]],
    ):
        self.handlers = handlers

    def create_job(self, task: "Task") -> T_SimpleJobClass:
        task_function = task.data["function"]
        handler_class = self.handlers[task_function]

        return handler_class(task)
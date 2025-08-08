from abc import ABC, abstractmethod
from typing import Generic, Type, TypeVar

from yatq.dto import Task
from yatq.worker.job.base import BaseJob

T_BaseJob = TypeVar("T_BaseJob", bound=BaseJob)


class BaseJobFactory(ABC, Generic[T_BaseJob]):
    def __init__(self, **kwargs):
        super().__init__()

    @abstractmethod
    def get_job_class(self, task: Task) -> Type[T_BaseJob]: ...

    @abstractmethod
    def create_job(self, task: Task) -> T_BaseJob: ...

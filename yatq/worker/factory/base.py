from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from yatq.worker.job.base import BaseJob

if TYPE_CHECKING:  # pragma: no cover
    from yatq.dto import Task


class BaseJobFactory(ABC):
    @abstractmethod
    def create_job(self, task: "Task") -> BaseJob:  # pragma: no cover
        ...

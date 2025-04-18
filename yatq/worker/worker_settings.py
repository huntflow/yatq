from abc import ABC, abstractmethod
from types import TracebackType
from typing import Awaitable, Callable, Dict, Optional, Tuple, Type

from redis.asyncio import Redis

from yatq.worker.factory.base import BaseJobFactory
from yatq.worker.factory.simple import SimpleJobFactory
from yatq.worker.job.base import BaseJob

T_ExcInfo = Tuple[Type[BaseException], BaseException, TracebackType]
T_ExceptionHandler = Callable[[BaseJob, T_ExcInfo], Awaitable]


class WorkerSettings(ABC):
    """
    WorkerSettings class is used to configure worker.

    If you are using yatq-worker command line utility, you should
    implement custom config by inheriting this class and implementing
    `redis_client` method, along with setting `factory_kwargs` and/or
    custom `factory_cls`.
    """

    factory_cls: Type[BaseJobFactory] = SimpleJobFactory
    factory_kwargs: Optional[Dict] = None

    queue_namespace: Optional[str] = None

    @staticmethod
    async def on_startup() -> None:  # pragma: no cover
        ...

    @staticmethod
    async def on_shutdown() -> None:  # pragma: no cover
        ...

    @staticmethod
    @abstractmethod
    async def redis_client() -> Redis:  # pragma: no cover
        ...

    @staticmethod
    async def on_task_process_exception(
        job: BaseJob,
        exc_info: T_ExcInfo,
    ) -> None:  # pragma: no cover
        ...

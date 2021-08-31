from types import TracebackType
from typing import Dict, Optional, Tuple, Type

import aioredis

from yatq.worker.factory.base import BaseJobFactory
from yatq.worker.factory.simple import SimpleJobFactory
from yatq.worker.job.base import BaseJob

T_ExcInfo = Tuple[Type[BaseException], BaseException, TracebackType]


# The whole concept with the settings class looks a little bit ugly
# I suggest the following.
# Define default settings class here (without factory_kwargs)
# Use the default class explicitely in cli module.
# Define and use (with a straightforward import) whatever needed settings
# class in client code (client of the yatq lib).
# Drop `utils` module.
class WorkerSettings:

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
    async def redis_client() -> aioredis.Redis:  # pragma: no cover
        ...

    @staticmethod
    async def on_task_process_exception(
        job: BaseJob,
        exc_info: T_ExcInfo,
    ) -> None:  # pragma: no cover
        ...

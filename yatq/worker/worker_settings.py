from typing import Dict, Optional, Type

import aioredis

from yatq.worker.factory.simple import SimpleJobFactory


class WorkerSettings:

    factory_cls: Type[SimpleJobFactory] = SimpleJobFactory
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

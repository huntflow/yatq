import json
import logging
from hashlib import sha1
from string import Template
from typing import Any, Dict

try:
    from aioredis import Redis
except ImportError:
    from redis.asyncio import Redis  # type: ignore

from yatq.redis_compat import NoScriptError, eval_sha  # type: ignore

logger = logging.getLogger(__name__)


class LuaFunction:
    def __init__(self, template: str, environment: Dict[str, Any]):
        self.template = template
        self.environment = environment

        self.lua: str = Template(self.template).substitute(**self.environment)
        self.digest: str = sha1(self.lua.encode()).hexdigest()

    async def _store_function(self, client: Redis):
        logger.debug("Calling SCRIPT LOAD")
        await client.script_load(self.lua)

    async def _call_cached(self, client: Redis, *args):
        return await eval_sha(client, self.digest, args)

    async def call(self, client: Redis, *args):
        try:
            result = await self._call_cached(client, *args)
        except NoScriptError:
            logger.debug("Received NOSCRIPT")
            await self._store_function(client)
            result = await self._call_cached(client, *args)

        logger.debug("EVALSHA result: %s", result)

        return json.loads(result)

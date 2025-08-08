import json
from hashlib import sha1
from string import Template
from typing import Any, Dict, Optional

from redis.asyncio import Redis
from redis.commands.core import AsyncScript


class LuaFunction:
    def __init__(self, template: str, environment: Dict[str, Any]):
        self.template = template
        self.environment = environment
        self.script: Optional[AsyncScript] = None

    @property
    def lua(self) -> str:
        template = Template(self.template)
        return template.substitute(**self.environment)

    @property
    def digest(self) -> str:
        return sha1(self.lua.encode()).hexdigest()

    async def call(self, client: Redis, *args):
        if self.script is None:
            self.script = client.register_script(self.lua)
        result = await self.script(args=args)

        return json.loads(result)

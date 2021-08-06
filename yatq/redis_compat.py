# type: ignore
from typing import Any, Tuple

import aioredis

if aioredis.__version__ >= "2.0":  # pragma: no cover
    from aioredis.exceptions import NoScriptError

    async def eval_sha(
        client: aioredis.Redis, digest: str, args: Tuple[Any]
    ):  # pragma: no cover
        return await client.evalsha(digest, 0, *args)


else:  # pragma: no cover
    from aioredis.errors import ReplyError

    class NoScriptError(ReplyError):
        ...

    async def eval_sha(
        client: aioredis.Redis, digest: str, args: Tuple[Any]
    ):  # pragma: no cover
        try:
            return await client.evalsha(digest, keys=[], args=list(args))
        except ReplyError as error:
            if error.args[0].startswith("NOSCRIPT"):
                raise NoScriptError(error.args[0])
            raise

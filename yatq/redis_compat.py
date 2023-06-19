# type: ignore
from typing import Any, Tuple
from yatq.py_version import AIOREDIS_USE

if AIOREDIS_USE: # pragma: no cover
    import aioredis

    if aioredis.__version__ >= "2.0":
        from aioredis.exceptions import NoScriptError

        async def eval_sha(
            client: aioredis.Redis, digest: str, args: Tuple[Any]
        ):  # pragma: no cover
            return await client.evalsha(digest, 0, *args)

    else:
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

else: # pragma: no cover
    from redis import asyncio as aioredis
    from redis.exceptions import NoScriptError

    async def eval_sha(
        client: aioredis.Redis, digest: str, args: Tuple[Any]
    ):  # pragma: no cover
        # return await client.evalsha(digest, 0, *args)
        return await client.execute_command("EVALSHA", digest, 0, *args)

from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:  # pragma: no cover
    from yatq.dto import Task


class BaseTaskRunner:

    def __init__(self, ctx: Dict[str, Any], task: "Task") -> None:
        self.ctx = ctx
        self.task = task

    async def process(self) -> None:
        await self.pre_run()

        try:
            await self.run()
        finally:
            await self.post_run()

    async def pre_run(self) -> None:
        ...

    async def run(self) -> None:
        ...

    async def post_run(self) -> None:
        ...

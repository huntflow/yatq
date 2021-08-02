from time import time
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:  # pragma: no cover
    from yatq.dto import Task


class BaseTaskRunner:
    def __init__(self, ctx: Dict[str, Any], task: "Task") -> None:
        self.ctx = ctx
        self.task = task

        self.run_start: Optional[int] = None
        self.run_stop: Optional[int] = None

        self.post_process_start: Optional[int] = None
        self.post_process_stop: Optional[int] = None

    @property
    def process_duration(self) -> int:
        if not (self.run_start and self.run_stop):
            return 0

        return self.run_stop - self.run_start

    @property
    def post_process_duration(self) -> int:
        if not (self.post_process_start and self.post_process_stop):
            return 0

        return self.post_process_stop - self.post_process_start

    async def process(self) -> None:
        await self.pre_run()

        try:
            self.run_start = time()
            await self.run()
        finally:
            self.run_stop = time()
            await self.post_run()

    async def do_post_process(self) -> None:
        self.post_process_start = time()
        try:
            await self.post_process()
        finally:
            self.post_process_stop = time()

    async def post_process(self) -> None:
        ...

    async def pre_run(self) -> None:
        ...

    async def run(self) -> None:
        ...

    async def post_run(self) -> None:
        ...

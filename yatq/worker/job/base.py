from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, contextmanager
from time import time
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterator, Optional

if TYPE_CHECKING:  # pragma: no cover
    from yatq.dto import Task


class BaseJob(ABC):
    def __init__(self, task: "Task") -> None:
        self.task = task

        self.run_start: Optional[float] = None
        self.run_stop: Optional[float] = None

        self.post_process_start: Optional[float] = None
        self.post_process_stop: Optional[float] = None

        self.result = None

    @property
    def process_duration(self) -> float:
        if not (self.run_start and self.run_stop):
            return 0.0

        return round(self.run_stop - self.run_start, 4)

    @property
    def post_process_duration(self) -> float:
        if not (self.post_process_start and self.post_process_stop):
            return 0.0

        return round(self.post_process_stop - self.post_process_start, 4)

    @classmethod
    def format_result(cls, result: Any) -> str:
        return str(result)

    async def process(self) -> None:
        async with self.run_context():
            with self.run_timer():
                result = await self.run()
                self.task.result = self.format_result(result)

    async def do_post_process(self) -> None:
        with self.post_process_timer():
            await self.post_process()

    # NOTE: mypy sees context managers as iterators. Thus, weird annotations

    @contextmanager
    def run_timer(self) -> Iterator[None]:
        self.run_start = time()
        try:
            yield
        finally:
            self.run_stop = time()

    @contextmanager
    def post_process_timer(self) -> Iterator[None]:
        self.post_process_start = time()
        try:
            yield
        finally:
            self.post_process_stop = time()

    @asynccontextmanager
    async def run_context(self) -> AsyncIterator[None]:
        yield

    @abstractmethod
    async def run(self) -> Any:  # pragma: no cover
        ...

    async def post_process(self) -> None:
        ...

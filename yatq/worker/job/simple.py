import json
from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from yatq.worker.job.base import BaseJob

if TYPE_CHECKING:  # pragma: no cover
    from yatq.dto import Task


class SimpleJob(BaseJob):
    def __init__(self, task: "Task") -> None:
        super().__init__(task)

        task_data = task.data
        if not task_data:
            raise ValueError("Task data is not set")

        self.kwargs = task_data["kwargs"]

    @classmethod
    def format_result(cls, result: Any) -> str:
        try:
            formatted_result = json.dumps(result)
        except Exception:
            formatted_result = json.dumps(str(result))

        return formatted_result

    async def process(self) -> None:
        async with self.run_context(), self.run_timer():
            result = await self.run(**self.kwargs)
            self.task.result = self.format_result(result)

    async def do_post_process(self) -> None:
        async with self.post_process_timer():
            await self.post_process(**self.kwargs)

    @abstractmethod
    async def run(self, **kwargs) -> Any:  # pragma: no cover
        ...

    async def post_process(self, **kwargs) -> None:
        ...

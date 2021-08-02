from typing import Any, Awaitable, Callable, Dict, Optional

from .task_runner import BaseTaskRunner

T_HOOK = Callable[[Dict[Any, Any]], Awaitable[None]]


class WorkerSettings:

    on_startup: Optional[T_HOOK] = None
    on_shutdown: Optional[T_HOOK] = None

    handlers: Dict[str, BaseTaskRunner]

import argparse
import asyncio
import logging.config
import signal
from typing import Callable, Coroutine, Dict, List, Optional, Type

from yatq.defaults import DEFAULT_LOGGING_CONFIG
from yatq.worker.runner import build_worker
from yatq.worker.utils import import_string
from yatq.worker.worker_settings import WorkerSettings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("Yet Another Task Queue worker")
    parser.add_argument(
        "settings_module", help="Config class path, i.e. my_module.WorkerConfig"
    )
    parser.add_argument(
        "queue_names",
        metavar="Q",
        type=str,
        nargs="+",
        help="queue names for processing",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        help="Max async jobs per worker instance",
    )

    return parser


def run_worker_cli() -> None:  # pragma: no cover
    parser = build_parser()
    args = parser.parse_args()

    worker_settings: Type[WorkerSettings] = import_string(args.settings_module)

    run(
        worker_settings=worker_settings,
        queue_names=args.queue_names,
        max_jobs=args.max_jobs,
    )


async def async_run(
    worker_settings: Type[WorkerSettings],
    queue_names: List[str],
    logging_config: Optional[Dict] = None,
    max_jobs: Optional[int] = None,
    healtcheck: Optional[Callable[[], Coroutine]] = None,
) -> None:  # pragma: no cover
    logging_config = logging_config or DEFAULT_LOGGING_CONFIG
    logging.config.dictConfig(logging_config)

    loop = asyncio.get_event_loop()

    await worker_settings.on_startup()
    try:
        redis_client = await worker_settings.redis_client()
        worker = build_worker(
            redis_client,
            worker_settings.factory_cls,
            worker_settings.factory_kwargs,
            queue_names,
            max_jobs=max_jobs,
            queue_namespace=worker_settings.queue_namespace,
            on_task_process_exception=worker_settings.on_task_process_exception,
            healtcheck=healtcheck,
        )

        stop_signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for signum in stop_signals:
            loop.add_signal_handler(signum, lambda: asyncio.create_task(worker.stop()))

        await worker.run()
    finally:
        await worker_settings.on_shutdown()


def run(
    worker_settings: Type[WorkerSettings],
    queue_names: List[str],
    logging_config: Optional[Dict] = None,
    max_jobs: Optional[int] = None,
    healtcheck: Optional[Callable[[], Coroutine]] = None,
) -> None:  # pragma: no cover
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        async_run(
            worker_settings=worker_settings,
            queue_names=queue_names,
            logging_config=logging_config,
            max_jobs=max_jobs,
            healtcheck=healtcheck,
        ),
    )

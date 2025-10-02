import asyncio
import logging.config
import sys
import traceback
from contextvars import copy_context
from datetime import datetime
from inspect import isawaitable
from typing import (
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    cast,
)

from redis.asyncio import Redis

from yatq.defaults import (
    DEFAULT_MAX_JOBS,
    DEFAULT_QUEUE_NAMESPACE,
    DEFAULT_TASK_EXPIRATION,
)
from yatq.dto import RunningTaskState, TaskWrapper, WorkerState
from yatq.enums import TaskState
from yatq.exceptions import RetryTask, TaskRescheduleException
from yatq.queue import Queue
from yatq.vars import JOB_HANDLER, JOB_ID
from yatq.worker.factory.base import BaseJobFactory
from yatq.worker.worker_settings import T_ExceptionHandler, T_ExcInfo

LOGGER = logging.getLogger("yatq.worker")
LOGGER.setLevel("INFO")
GRAVEKEEPER_LOGGER = logging.getLogger("yatq.gravekeeper")
PROFILING_LOGGER = logging.getLogger("yatq.profiling")
PROFILING_LOGGER.propagate = False


async def _healthcheck_stub() -> None:
    pass


class Worker:
    def __init__(
        self,
        queue_list: List[Queue],
        task_factory: BaseJobFactory,
        on_task_process_exception: Optional[T_ExceptionHandler] = None,
        poll_interval: float = 2.0,
        max_jobs: int = 8,
        gravekeeper_interval: float = 30.0,
        profiling_interval: Optional[float] = None,
        on_stop_handlers: Optional[List[Coroutine]] = None,
        exit_after_jobs: Optional[int] = None,
        healtchcheck: Optional[Callable[[], Coroutine]] = None,
    ) -> None:
        self.queue_list = queue_list
        self.task_factory = task_factory

        # NOTE: events are currently only used for testing.
        # Do not rely on them in production code.
        self.started = asyncio.Event()
        self.got_task = asyncio.Event()
        self.completed_task = asyncio.Event()

        self._poll_event = asyncio.Event()
        self._poll_interval = poll_interval

        self._gravekeeper_interval = gravekeeper_interval

        self._stop_event = asyncio.Event()

        self._max_jobs = max_jobs
        self._exit_after_jobs = exit_after_jobs
        self._total_jobs_taken = 0
        self._job_handlers: Dict[asyncio.Task, Tuple[TaskWrapper, Queue]] = {}

        self._on_task_process_exception = on_task_process_exception
        self._profiling_interval = profiling_interval
        self._on_stop_handlers = on_stop_handlers

        self._gravekeeper_task: Optional[asyncio.Task] = None
        self._profiling_task: Optional[asyncio.Task] = None
        self._periodic_poll_task: Optional[asyncio.Task] = None
        self._exit_message: Optional[str] = None
        self._healtchcheck: Callable[[], Coroutine] = healtchcheck or _healthcheck_stub

    @property
    def should_get_new_task(self) -> bool:
        return len(self._job_handlers) < self._max_jobs

    async def _periodic_poll(self) -> None:
        while True:
            self._poll_event.set()
            await asyncio.sleep(self._poll_interval)

    async def _wait_poll(self) -> None:
        _, pending = await asyncio.wait(
            {
                asyncio.create_task(self._poll_event.wait()),
                asyncio.create_task(self._stop_event.wait()),
            },
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        self._poll_event.clear()

    async def _run_gravekeeper(self):
        while True:
            await self._call_gravekeeper()
            await asyncio.sleep(self._gravekeeper_interval)

    async def _call_gravekeeper(self):
        for queue in self.queue_list:
            buried_count = await queue.bury_tasks()

            if buried_count:
                GRAVEKEEPER_LOGGER.warning(
                    "Buried %s tasks in queue '%s'", buried_count, queue.name
                )

    async def _try_fetch_task(self) -> bool:
        for queue in self.queue_list:
            LOGGER.debug("Requesting new task from queue %s", queue.name)

            try:
                wrapper: Optional[TaskWrapper] = await queue.get_task()
            except Exception:  # pragma: no cover
                LOGGER.exception("Error getting task from queue %s", queue.name)
                # Checking connectivity
                if not await queue.check_connection():
                    LOGGER.warning("Queue %s in unavailable; stopping", queue.name)
                    await self.stop("Redis connection lost")
                    return False
                continue

            if not wrapper:
                continue

            await self._start_task_processing(wrapper, queue)
            return True

        return False

    async def _start_task_processing(self, wrapper: TaskWrapper, queue: Queue) -> None:
        LOGGER.info("Got task %s", wrapper.summary)
        LOGGER.debug("Task data: %s", wrapper.task.encoded_data)

        self.got_task.clear()
        self.got_task.set()

        handle_task = asyncio.create_task(self._handle_task(wrapper, queue))
        handle_task.add_done_callback(self._remove_completed_handle_task)
        self._job_handlers[handle_task] = (wrapper, queue)

    def _remove_completed_handle_task(self, task: asyncio.Task) -> None:
        self._job_handlers.pop(task)
        self.completed_task.clear()
        self.completed_task.set()

    async def _handle_task(self, wrapper: TaskWrapper, queue: Queue) -> None:
        task_id = wrapper.task.id
        try:
            task_job = self.task_factory.create_job(wrapper.task)
        except Exception:
            LOGGER.exception(
                "Failed to create job for task %s (%s-%s)",
                wrapper.task.data,
                task_id,
                wrapper.key,
            )
            await queue.fail_task(wrapper)
            return

        job_name = task_job.__class__.__name__
        JOB_HANDLER.set(job_name)
        try:
            # Wrapping coroutine in asyncio.task to copy contextvars
            job_coro = task_job.process()
        except Exception:
            LOGGER.exception(
                "Failed to create job '%s' (%s) coroutine", job_name, task_id
            )
            await queue.fail_task(wrapper)
            return
        context = copy_context()
        context.run(JOB_ID.set, wrapper.task.id)
        context.run(JOB_HANDLER.set, job_name)
        job_task = context.run(asyncio.create_task, job_coro)

        LOGGER.info("Starting job '%s' (%s)", job_name, wrapper.task.id)
        try:
            await job_task
            job_task.result()
        except RetryTask as retry_exc:
            LOGGER.info("Retrying job '%s' (%s)", job_name, task_id)
            await self._try_reschedule_task(wrapper, queue, force=retry_exc.force)
            return
        except Exception:
            LOGGER.exception("Exception in job '%s' (%s)", job_name, task_id)
            wrapper.task.result = {"traceback": traceback.format_exc()}

            exc_info = cast(T_ExcInfo, sys.exc_info())
            try:
                if self._on_task_process_exception:
                    await self._on_task_process_exception(task_job, exc_info)
            except Exception:
                LOGGER.exception(
                    "Exception in exception handler for job '%s' (%s)",
                    job_name,
                    task_id,
                )

            await self._try_reschedule_task(wrapper, queue)
            return

        wrapper.task.state = TaskState.COMPLETED
        wrapper.task.finished = datetime.now()
        await queue.complete_task(wrapper)

        try:
            await context.run(task_job.do_post_process)
        except Exception:
            LOGGER.exception(
                "Exception in job '%s' (%s) post processing",
                job_name,
                task_id,
            )

        LOGGER.info(
            "Finished job '%s' (%s) after %s seconds (%s seconds postprocessing) with state %s",
            job_name,
            task_id,
            task_job.process_duration,
            task_job.post_process_duration,
            wrapper.task.state.value,
        )

    async def _try_reschedule_task(
        self, wrapped_task: TaskWrapper, queue: Queue, force: bool = False
    ):
        task_id = wrapped_task.task.id
        try:
            scheduled_after = await queue.auto_reschedule_task(
                wrapped_task, force=force
            )
        except TaskRescheduleException as error:
            LOGGER.warning("Failed to reschedule task %s: %s", task_id, error)
        else:
            LOGGER.info(
                "Rescheduling task %s, next try after %s seconds",
                task_id,
                scheduled_after,
            )

    async def _complete_pending_jobs(self):
        if not self._job_handlers:
            LOGGER.info("No running jobs; exiting")
            return

        LOGGER.info("Waiting for %s running job(s) to finish", len(self._job_handlers))
        await asyncio.wait(self._job_handlers, return_when=asyncio.ALL_COMPLETED)
        LOGGER.info("All jobs are completed.")

    async def stop(self, exit_message: Optional[str] = None) -> None:
        LOGGER.info("Stopping worker")
        self._stop_event.set()
        self._poll_event.set()
        self._exit_message = exit_message

    def _increment_jobs_taken_counter(self) -> None:
        """
        Counts total number of jobs taken by worker and triggers stop event if limit is reached
        """
        self._total_jobs_taken += 1
        if not self._exit_after_jobs:
            return

        if self._total_jobs_taken >= self._exit_after_jobs:
            LOGGER.warning(
                "Job limit reached (%s of %s); stopping worker",
                self._total_jobs_taken,
                self._exit_after_jobs,
            )
            self._stop_event.set()

    async def wait_stopped(self) -> Optional[str]:
        await self._complete_pending_jobs()
        if self._periodic_poll_task:
            self._periodic_poll_task.cancel()
        if self._gravekeeper_task:
            self._gravekeeper_task.cancel()
        await self.stop_profiler()
        return self._exit_message

    async def run(self) -> Optional[str]:
        LOGGER.info(
            "Starting worker, queue list: %s", [q.name for q in self.queue_list]
        )
        self._stop_event.clear()
        self.started.clear()
        self.started.set()

        self._periodic_poll_task = asyncio.create_task(self._periodic_poll())
        self._gravekeeper_task = asyncio.create_task(self._run_gravekeeper())
        await self.start_profiler()
        while not self._stop_event.is_set():
            await self._healtchcheck()
            if self.should_get_new_task:
                fetched = await self._try_fetch_task()
                if fetched:
                    self._increment_jobs_taken_counter()
                    continue
            await self._wait_poll()

        if self._on_stop_handlers:
            await asyncio.gather(*self._on_stop_handlers)

        return await self.wait_stopped()

    async def _run_profiler(self):
        import tracemalloc
        from collections import Counter

        tracemalloc.start()

        while True:
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics("lineno")[:10]

            PROFILING_LOGGER.info("Memory stats:")
            for entry in top_stats:
                PROFILING_LOGGER.info(str(entry))

            coros = Counter()
            for task in asyncio.Task.all_tasks():
                coro_name = task.get_coro().__qualname__
                coros[coro_name] += 1

            PROFILING_LOGGER.info("Total running tasks: %s", sum(coros.values()))
            for coro_name, coro_count in coros.most_common(10):
                PROFILING_LOGGER.info(
                    "Coroutine %s: %s object(s)", coro_name, coro_count
                )

            await asyncio.sleep(self._profiling_interval)

    async def start_profiler(self):
        if not self._profiling_interval:
            return

        LOGGER.info("Starting profiler")
        self._profiling_task = asyncio.create_task(self._run_profiler())

    async def stop_profiler(self):
        if not self._profiling_task:
            return

        self._profiling_task.cancel()
        try:
            await self._profiling_task
        except asyncio.CancelledError:
            pass

        self._profiling_task = None

    def get_state(self) -> WorkerState:
        task_state = []
        for aio_task, (wrapper, queue) in self._job_handlers.items():
            task_id = wrapper.task.id
            task_key = wrapper.key
            task_data = wrapper.task.data
            handler = self.task_factory.get_job_class(wrapper.task)
            task_handler = (
                handler.__class__.__module__ + "." + handler.__class__.__qualname__
            )
            task_frames = [str(frame) for frame in aio_task.get_stack()]

            coro = aio_task.get_coro()
            coro_stack = []
            while True:
                try:
                    frame = coro.cr_frame  # type: ignore
                except AttributeError:
                    frame = coro.gi_frame  # type: ignore

                coro_stack.append(str(frame))

                try:
                    coro = coro.cr_await  # type: ignore
                except AttributeError:
                    coro = coro.gi_yieldfrom  # type: ignore
                if not isawaitable(coro):
                    break

            task_state.append(
                RunningTaskState(
                    id=task_id,
                    key=task_key,
                    data=task_data,
                    taken_at=wrapper.taken_at,
                    handler=task_handler,
                    task_stack=task_frames,
                    coro_stack=coro_stack,
                )
            )

        max_tasks = self._max_jobs
        current_task_count = len(self._job_handlers)
        return WorkerState(
            max_tasks=max_tasks,
            current_task_count=current_task_count,
            current_task_state=task_state,
        )


def build_worker(
    redis_client: Redis,
    factory_cls: Type[BaseJobFactory],
    factory_kwargs: Optional[Dict],
    queue_names: List[str],
    queue_namespace: Optional[str] = None,
    max_jobs: Optional[int] = None,
    on_task_process_exception: Optional[T_ExceptionHandler] = None,
    default_ttl: int = DEFAULT_TASK_EXPIRATION,
    profiling_interval: Optional[float] = None,
    on_stop_handlers: Optional[List[Coroutine]] = None,
    poll_interval: float = 2.0,
    exit_after_jobs: Optional[int] = None,
    healtchcheck: Optional[Callable[[], Coroutine]] = None,
) -> Worker:
    factory_kwargs = factory_kwargs or {}
    task_factory = factory_cls(**factory_kwargs)

    queue_namespace = queue_namespace or DEFAULT_QUEUE_NAMESPACE
    max_jobs = max_jobs or DEFAULT_MAX_JOBS

    queue_list: List[Queue] = [
        Queue(
            client=redis_client,
            name=queue_name,
            namespace=queue_namespace,
            default_ttl=default_ttl,
        )
        for queue_name in queue_names
    ]
    worker = Worker(
        queue_list=queue_list,
        task_factory=task_factory,
        max_jobs=max_jobs,
        on_task_process_exception=on_task_process_exception,
        profiling_interval=profiling_interval,
        on_stop_handlers=on_stop_handlers,
        exit_after_jobs=exit_after_jobs,
        poll_interval=poll_interval,
        healtchcheck=healtchcheck,
    )

    return worker

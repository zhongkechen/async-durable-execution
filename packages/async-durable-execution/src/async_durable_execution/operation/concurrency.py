"""Concurrent executor for parallel and map operations."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ..models import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    BranchStatus,
    Executable,
    ExecutableWithState,
    ExecutionCounters,
    SuspendResult,
)
from ..config import ChildConfig, NestingType
from ..exceptions import (
    OrphanedChildException,
    SuspendExecution,
    TimedSuspendExecution,
)
from ..models import ErrorObject, OperationIdentifier
from .child import child_handler


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ..config import CompletionConfig
    from .child import DurableContext
    from ..models import OperationSubType
    from ..serdes import SerDes
    from ..state import ExecutionState
    from ..types import SummaryGenerator


logger = logging.getLogger(__name__)

CallableType = TypeVar("CallableType")
ResultType = TypeVar("ResultType")


class TimerScheduler:
    """Manage timed suspend tasks with event-loop tasks."""

    def __init__(
        self,
        resubmit_callback: Callable[[ExecutableWithState[Any, Any]], Awaitable[None]],
    ) -> None:
        self.resubmit_callback = resubmit_callback
        self._resume_tasks: set[asyncio.Task[None]] = set()

    async def __aenter__(self) -> TimerScheduler:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()

    def schedule_resume(
        self,
        exe_state: ExecutableWithState[CallableType, ResultType],
        resume_time: float,
    ) -> None:
        async def runner() -> None:
            await asyncio.sleep(max(0.0, resume_time - time.time()))
            if exe_state.can_resume:
                exe_state.reset_to_pending()
                await self.resubmit_callback(exe_state)

        task = asyncio.create_task(runner())
        self._resume_tasks.add(task)
        task.add_done_callback(self._resume_tasks.discard)

    async def shutdown(self) -> None:
        tasks = list(self._resume_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._resume_tasks.clear()


class ConcurrentExecutor(ABC, Generic[CallableType, ResultType]):
    """Execute durable operations concurrently using asyncio tasks."""

    def __init__(
        self,
        executables: list[Executable[CallableType]],
        max_concurrency: int | None,
        completion_config: CompletionConfig,
        sub_type_top: OperationSubType,
        sub_type_iteration: OperationSubType,
        name_prefix: str,
        serdes: SerDes | None,
        item_serdes: SerDes | None = None,
        summary_generator: SummaryGenerator | None = None,
        nesting_type: NestingType = NestingType.NESTED,
    ):
        self.executables = executables
        self.max_concurrency = max_concurrency
        self.completion_config = completion_config
        self.sub_type_top = sub_type_top
        self.sub_type_iteration = sub_type_iteration
        self.name_prefix = name_prefix
        self.summary_generator = summary_generator
        self.nesting_type = nesting_type
        self._completion_event = asyncio.Event()
        self._suspend_exception: SuspendExecution | None = None
        self._running_tasks: set[asyncio.Task[ResultType]] = set()

        min_successful = self.completion_config.min_successful or len(self.executables)
        self.counters = ExecutionCounters(
            len(executables),
            min_successful,
            self.completion_config.tolerated_failure_count,
            self.completion_config.tolerated_failure_percentage,
        )
        self.executables_with_state: list[ExecutableWithState] = []
        self.serdes = serdes
        self.item_serdes = item_serdes

    @abstractmethod
    async def execute_item(
        self,
        child_context: DurableContext,
        executable: Executable[CallableType],
    ) -> ResultType:
        raise NotImplementedError

    def get_iteration_name(self, index: int) -> str:
        return f"{self.name_prefix}{index}"

    async def execute(
        self, execution_state: ExecutionState, executor_context: DurableContext
    ) -> BatchResult[ResultType]:
        logger.debug(
            "▶️ Executing concurrent operation, items: %d", len(self.executables)
        )

        if not self.executables:
            logger.debug("No items to execute, returning empty result")
            return self._create_result()

        max_workers = self.max_concurrency or len(self.executables)
        semaphore = asyncio.Semaphore(max_workers)
        self.executables_with_state = [
            ExecutableWithState(executable=exe) for exe in self.executables
        ]
        self._completion_event.clear()
        self._suspend_exception = None
        self._running_tasks.clear()

        async def submit_task(
            executable_with_state: ExecutableWithState[CallableType, ResultType],
        ) -> None:
            if self._completion_event.is_set():
                return

            async def run_task() -> ResultType:
                async with semaphore:
                    return await self._execute_item_in_child_context(
                        executor_context,
                        executable_with_state.executable,
                    )

            task = asyncio.create_task(run_task())
            executable_with_state.run(task)
            self._running_tasks.add(task)

            def on_done(done_task: asyncio.Task[ResultType]) -> None:
                self._running_tasks.discard(done_task)
                asyncio.create_task(
                    self._on_task_complete(
                        executable_with_state,
                        done_task,
                        scheduler,
                    )
                )

            task.add_done_callback(on_done)

        async def resubmitter(
            executable_with_state: ExecutableWithState[CallableType, ResultType],
        ) -> None:
            await execution_state._create_checkpoint_async(is_sync=False)
            await submit_task(executable_with_state)

        async with TimerScheduler(resubmitter) as scheduler:
            for exe_state in self.executables_with_state:
                await submit_task(exe_state)

            await self._completion_event.wait()

            for task in list(self._running_tasks):
                if not task.done():
                    task.cancel()
            if self._running_tasks:
                await asyncio.gather(*self._running_tasks, return_exceptions=True)

            if self._suspend_exception:
                raise self._suspend_exception

        return self._create_result()

    def should_execution_suspend(self) -> SuspendResult:
        earliest_timestamp: float = float("inf")
        indefinite_suspend_task: (
            ExecutableWithState[CallableType, ResultType] | None
        ) = None

        for exe_state in self.executables_with_state:
            if exe_state.status in {BranchStatus.PENDING, BranchStatus.RUNNING}:
                return SuspendResult.do_not_suspend()
            if exe_state.status is BranchStatus.SUSPENDED_WITH_TIMEOUT:
                if (
                    exe_state.suspend_until
                    and exe_state.suspend_until < earliest_timestamp
                ):
                    earliest_timestamp = exe_state.suspend_until
            elif exe_state.status is BranchStatus.SUSPENDED:
                indefinite_suspend_task = exe_state

        if earliest_timestamp != float("inf"):
            return SuspendResult.suspend(
                TimedSuspendExecution(
                    "All concurrent work complete or suspended pending retry.",
                    earliest_timestamp,
                )
            )
        if indefinite_suspend_task:
            return SuspendResult.suspend(
                SuspendExecution(
                    "All concurrent work complete or suspended and pending external callback."
                )
            )

        return SuspendResult.do_not_suspend()

    async def _on_task_complete(
        self,
        exe_state: ExecutableWithState[CallableType, ResultType],
        task: asyncio.Task[ResultType],
        scheduler: TimerScheduler,
    ) -> None:
        if task.cancelled():
            exe_state.suspend()
            return

        try:
            result = task.result()
            exe_state.complete(result)
            self.counters.complete_task()
        except OrphanedChildException:
            logger.debug(
                "Terminating orphaned branch %s without error because parent has completed already",
                exe_state.index,
            )
            return
        except TimedSuspendExecution as tse:
            exe_state.suspend_with_timeout(tse.scheduled_timestamp)
            scheduler.schedule_resume(exe_state, tse.scheduled_timestamp)
        except SuspendExecution:
            exe_state.suspend()
        except Exception as e:  # noqa: BLE001
            exe_state.fail(e)
            self.counters.fail_task()

        if self.counters.should_complete():
            self._completion_event.set()
        else:
            suspend_result = self.should_execution_suspend()
            if suspend_result.should_suspend:
                self._suspend_exception = suspend_result.exception
                self._completion_event.set()

    def _create_result(self) -> BatchResult[ResultType]:
        batch_items: list[BatchItem[ResultType]] = []
        for executable in self.executables_with_state:
            match executable.status:
                case BranchStatus.COMPLETED:
                    batch_items.append(
                        BatchItem(
                            executable.index,
                            BatchItemStatus.SUCCEEDED,
                            executable.result,
                        )
                    )
                case BranchStatus.FAILED:
                    batch_items.append(
                        BatchItem(
                            executable.index,
                            BatchItemStatus.FAILED,
                            error=ErrorObject.from_exception(executable.error),
                        )
                    )
                case (
                    BranchStatus.PENDING
                    | BranchStatus.RUNNING
                    | BranchStatus.SUSPENDED
                    | BranchStatus.SUSPENDED_WITH_TIMEOUT
                ):
                    batch_items.append(
                        BatchItem(executable.index, BatchItemStatus.STARTED)
                    )

        return BatchResult.from_items(batch_items, self.completion_config)

    async def _execute_item_in_child_context(
        self,
        executor_context: DurableContext,
        executable: Executable[CallableType],
    ) -> ResultType:
        operation_id: str = (
            executor_context.step_counter._create_step_id_for_logical_step(  # noqa: SLF001
                executable.index
            )
        )
        name: str = self.get_iteration_name(executable.index)
        is_virtual: bool = self.nesting_type is NestingType.FLAT

        child_context: DurableContext = executor_context.create_child_context(
            operation_id, is_virtual=is_virtual
        )
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=self.sub_type_iteration,
            parent_id=executor_context.parent_id,  # noqa: SLF001
            name=name,
        )

        async def run_in_child_handler() -> ResultType:
            return await self.execute_item(child_context, executable)

        result = await child_handler(
            run_in_child_handler,
            child_context.execution_state,
            operation_identifier=operation_identifier,
            config=ChildConfig(
                serdes=self.item_serdes or self.serdes,
                sub_type=self.sub_type_iteration,
                summary_generator=self.summary_generator,
                is_virtual=is_virtual,
            ),
        )
        child_context.execution_state.track_replay(operation_id=operation_id)
        return result

    async def replay(
        self, execution_state: ExecutionState, executor_context: DurableContext
    ) -> BatchResult[ResultType]:
        items: list[BatchItem[ResultType]] = []
        for executable in self.executables:
            operation_id = (
                executor_context.step_counter._create_step_id_for_logical_step(  # noqa: SLF001
                    executable.index
                )
            )
            checkpoint = execution_state.get_checkpoint_result(operation_id)

            result: ResultType | None = None
            error = None
            status: BatchItemStatus
            if checkpoint.is_succeeded():
                status = BatchItemStatus.SUCCEEDED
                result = await self._execute_item_in_child_context(
                    executor_context, executable
                )
            elif checkpoint.is_failed():
                error = checkpoint.error
                status = BatchItemStatus.FAILED
            else:
                status = BatchItemStatus.STARTED

            items.append(
                BatchItem(executable.index, status, result=result, error=error)
            )
        return BatchResult.from_items(items, self.completion_config)

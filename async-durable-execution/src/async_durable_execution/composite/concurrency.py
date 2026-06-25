"""Concurrent executor for parallel and map operations."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ..exceptions import SuspendExecution, TimedSuspendExecution
from ..exceptions import InvalidStateError
from ..models import ErrorObject, OperationIdentifier, SerializableModel, _metadata
from ..primitive.base import get_checkpoint_result
from ..primitive.child import ChildConfig, OrphanedChildException, child_handler
from ..serdes import deserialize


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ..primitive.child import DurableContext
    from ..models import OperationSubType
    from ..serdes import SerDes
    from ..state import ExecutionState
    from ..types import SummaryGenerator


logger = logging.getLogger(__name__)

CallableType = TypeVar("CallableType")
ResultType = TypeVar("ResultType")
R = TypeVar("R")


class NestingType(Enum):
    """Control how child contexts are created for batch operations."""

    NESTED = "NESTED"
    FLAT = "FLAT"


@dataclass(frozen=True)
class CompletionConfig:
    """Configuration for determining when parallel/map operations complete."""

    min_successful: int | None = None
    tolerated_failure_count: int | None = None
    tolerated_failure_percentage: int | float | None = None

    @staticmethod
    def first_successful():
        return CompletionConfig(
            min_successful=1,
            tolerated_failure_count=None,
            tolerated_failure_percentage=None,
        )

    @staticmethod
    def all_completed():
        return CompletionConfig(
            min_successful=None,
            tolerated_failure_count=None,
            tolerated_failure_percentage=None,
        )

    @staticmethod
    def all_successful():
        return CompletionConfig(
            min_successful=None,
            tolerated_failure_count=0,
            tolerated_failure_percentage=0,
        )


class BatchItemStatus(Enum):
    """Status of one item or branch inside a batch-style operation."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STARTED = "STARTED"


class CompletionReason(Enum):
    """Why a map or parallel operation stopped collecting results."""

    ALL_COMPLETED = "ALL_COMPLETED"
    MIN_SUCCESSFUL_REACHED = "MIN_SUCCESSFUL_REACHED"
    FAILURE_TOLERANCE_EXCEEDED = "FAILURE_TOLERANCE_EXCEEDED"


@dataclass(frozen=True)
class SuspendResult:
    """Internal helper describing whether an executor should suspend."""

    should_suspend: bool
    exception: SuspendExecution | None = None

    @staticmethod
    def do_not_suspend() -> SuspendResult:
        return SuspendResult(should_suspend=False)

    @staticmethod
    def suspend(exception: SuspendExecution) -> SuspendResult:
        return SuspendResult(should_suspend=True, exception=exception)


@dataclass(frozen=True)
class BatchItem(SerializableModel, Generic[R]):
    """Result record for one branch or iteration in `BatchResult`."""

    index: int
    status: BatchItemStatus
    result: R | None = dataclass_field(
        default=None, metadata=_metadata(alias="result", omit_if_none=False)
    )
    error: ErrorObject | None = dataclass_field(
        default=None,
        metadata=_metadata(alias="error", omit_if_none=False),
    )


@dataclass(frozen=True)
class BatchResult(SerializableModel, Generic[R]):  # noqa: PYI059
    """Aggregated outcome of a `map()` or `parallel()` operation."""

    all: list[BatchItem[R]]
    completion_reason: CompletionReason = dataclass_field(
        metadata=_metadata(alias="completionReason")
    )

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], completion_config: CompletionConfig | None = None
    ) -> BatchResult[R]:
        batch_items = [BatchItem.from_dict(item) for item in data["all"]]

        completion_reason_value = data.get("completionReason")
        if completion_reason_value is None:
            result = cls.from_items(batch_items, completion_config)
            logger.warning(
                "Missing completionReason in BatchResult deserialization, "
                "inferred '%s' from batch item statuses. "
                "This may indicate incomplete serialization data.",
                result.completion_reason.value,
            )
            return result

        return cls(
            all=batch_items,
            completion_reason=CompletionReason(completion_reason_value),
        )

    @staticmethod
    def _get_completion_reason(
        failure_count: int,
        success_count: int,
        completed_count: int,
        total_count: int,
        completion_config: CompletionConfig | None,
    ) -> CompletionReason:
        if completion_config is None:
            if failure_count > 0:
                return CompletionReason.FAILURE_TOLERANCE_EXCEEDED
        else:
            has_any_completion_criteria = (
                completion_config.min_successful is not None
                or completion_config.tolerated_failure_count is not None
                or completion_config.tolerated_failure_percentage is not None
            )

            if not has_any_completion_criteria:
                if failure_count > 0:
                    return CompletionReason.FAILURE_TOLERANCE_EXCEEDED
            else:
                if (
                    completion_config.tolerated_failure_count is not None
                    and failure_count > completion_config.tolerated_failure_count
                ):
                    return CompletionReason.FAILURE_TOLERANCE_EXCEEDED

                if (
                    completion_config.tolerated_failure_percentage is not None
                    and total_count > 0
                ):
                    failure_percentage = (failure_count / total_count) * 100
                    if (
                        failure_percentage
                        > completion_config.tolerated_failure_percentage
                    ):
                        return CompletionReason.FAILURE_TOLERANCE_EXCEEDED

        if completed_count == total_count:
            return CompletionReason.ALL_COMPLETED

        if (
            completion_config is not None
            and completion_config.min_successful is not None
            and success_count >= completion_config.min_successful
        ):
            return CompletionReason.MIN_SUCCESSFUL_REACHED

        return CompletionReason.ALL_COMPLETED

    @classmethod
    def from_items(
        cls,
        items: list[BatchItem[R]],
        completion_config: CompletionConfig | None = None,
    ) -> BatchResult[R]:
        statuses = (item.status for item in items)
        counts = Counter(statuses)
        succeeded_count = counts.get(BatchItemStatus.SUCCEEDED, 0)
        failed_count = counts.get(BatchItemStatus.FAILED, 0)
        started_count = counts.get(BatchItemStatus.STARTED, 0)

        completed_count = succeeded_count + failed_count
        total_count = started_count + completed_count

        completion_reason = cls._get_completion_reason(
            failure_count=failed_count,
            success_count=succeeded_count,
            completed_count=completed_count,
            total_count=total_count,
            completion_config=completion_config,
        )

        return cls(all=items, completion_reason=completion_reason)

    def succeeded(self) -> list[BatchItem[R]]:
        return [
            item
            for item in self.all
            if item.status is BatchItemStatus.SUCCEEDED and item.result is not None
        ]

    def failed(self) -> list[BatchItem[R]]:
        return [
            item
            for item in self.all
            if item.status is BatchItemStatus.FAILED and item.error is not None
        ]

    def started(self) -> list[BatchItem[R]]:
        return [item for item in self.all if item.status is BatchItemStatus.STARTED]

    @property
    def status(self) -> BatchItemStatus:
        return BatchItemStatus.FAILED if self.has_failure else BatchItemStatus.SUCCEEDED

    @property
    def has_failure(self) -> bool:
        return any(item.status is BatchItemStatus.FAILED for item in self.all)

    def throw_if_error(self) -> None:
        first_error = next(
            (item.error for item in self.all if item.status is BatchItemStatus.FAILED),
            None,
        )
        if first_error:
            raise first_error.to_callable_runtime_error()

    def get_results(self) -> list[R]:
        return [
            item.result
            for item in self.all
            if item.status is BatchItemStatus.SUCCEEDED and item.result is not None
        ]

    def get_errors(self) -> list[ErrorObject]:
        return [
            item.error
            for item in self.all
            if item.status is BatchItemStatus.FAILED and item.error is not None
        ]

    @property
    def success_count(self) -> int:
        return sum(1 for item in self.all if item.status is BatchItemStatus.SUCCEEDED)

    @property
    def failure_count(self) -> int:
        return sum(1 for item in self.all if item.status is BatchItemStatus.FAILED)

    @property
    def started_count(self) -> int:
        return sum(1 for item in self.all if item.status is BatchItemStatus.STARTED)

    @property
    def total_count(self) -> int:
        return len(self.all)


@dataclass(frozen=True)
class Executable(Generic[CallableType]):
    """Index plus callable payload used by the concurrent executors."""

    index: int
    func: CallableType


class BranchStatus(Enum):
    """In-memory lifecycle state for a concurrently scheduled branch."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    SUSPENDED = "suspended"
    SUSPENDED_WITH_TIMEOUT = "suspended_with_timeout"
    FAILED = "failed"


class ExecutableWithState(Generic[CallableType, ResultType]):
    """Manages the execution state and lifecycle of an executable."""

    def __init__(self, executable: Executable[CallableType]):
        self.executable = executable
        self._status = BranchStatus.PENDING
        self._future: asyncio.Task[ResultType] | None = None
        self._suspend_until: float | None = None
        self._result: ResultType = None  # type: ignore[assignment]
        self._is_result_set = False
        self._error: Exception | None = None

    @property
    def future(self) -> asyncio.Task[ResultType]:
        if self._future is None:
            msg = f"ExecutableWithState was never started. {self.executable.index}"
            raise InvalidStateError(msg)
        return self._future

    @property
    def status(self) -> BranchStatus:
        return self._status

    @property
    def result(self) -> ResultType:
        if not self._is_result_set or self._status != BranchStatus.COMPLETED:
            msg = f"result not available in status {self._status}"
            raise InvalidStateError(msg)
        return self._result

    @property
    def error(self) -> Exception:
        if self._error is None or self._status != BranchStatus.FAILED:
            msg = f"error not available in status {self._status}"
            raise InvalidStateError(msg)
        return self._error

    @property
    def suspend_until(self) -> float | None:
        return self._suspend_until

    @property
    def is_running(self) -> bool:
        return self._status is BranchStatus.RUNNING

    @property
    def can_resume(self) -> bool:
        return self._status is BranchStatus.SUSPENDED or (
            self._status is BranchStatus.SUSPENDED_WITH_TIMEOUT
            and self._suspend_until is not None
            and time.time() >= self._suspend_until
        )

    @property
    def index(self) -> int:
        return self.executable.index

    @property
    def callable(self) -> CallableType:
        return self.executable.func

    def run(self, future: asyncio.Task[ResultType]) -> None:
        if self._status != BranchStatus.PENDING:
            msg = f"Cannot start running from {self._status}"
            raise InvalidStateError(msg)
        self._status = BranchStatus.RUNNING
        self._future = future

    def suspend(self) -> None:
        self._status = BranchStatus.SUSPENDED
        self._suspend_until = None

    def suspend_with_timeout(self, timestamp: float) -> None:
        self._status = BranchStatus.SUSPENDED_WITH_TIMEOUT
        self._suspend_until = timestamp

    def complete(self, result: ResultType) -> None:
        self._status = BranchStatus.COMPLETED
        self._result = result
        self._is_result_set = True

    def fail(self, error: Exception) -> None:
        self._status = BranchStatus.FAILED
        self._error = error

    def reset_to_pending(self) -> None:
        self._status = BranchStatus.PENDING
        self._future = None
        self._suspend_until = None


class ExecutionCounters:
    """Counters for tracking execution state on a single event loop."""

    def __init__(
        self,
        total_tasks: int,
        min_successful: int,
        tolerated_failure_count: int | None,
        tolerated_failure_percentage: float | None,
    ):
        self.total_tasks = total_tasks
        self.min_successful = min_successful
        self.tolerated_failure_count = tolerated_failure_count
        self.tolerated_failure_percentage = tolerated_failure_percentage
        self.success_count = 0
        self.failure_count = 0

    def complete_task(self) -> None:
        self.success_count += 1

    def fail_task(self) -> None:
        self.failure_count += 1

    def should_continue(self) -> bool:
        if (
            self.tolerated_failure_count is None
            and self.tolerated_failure_percentage is None
        ):
            return self.failure_count == 0

        if (
            self.tolerated_failure_count is not None
            and self.failure_count > self.tolerated_failure_count
        ):
            return False

        if self.tolerated_failure_percentage is not None and self.total_tasks > 0:
            failure_percentage = (self.failure_count / self.total_tasks) * 100
            if failure_percentage > self.tolerated_failure_percentage:
                return False

        return True

    def is_complete(self) -> bool:
        completed_count = self.success_count + self.failure_count

        if completed_count == self.total_tasks:
            return True

        return self.success_count >= self.min_successful

    def should_complete(self) -> bool:
        return self.is_complete() or not self.should_continue()

    def is_all_completed(self) -> bool:
        return self.success_count == self.total_tasks

    def is_min_successful_reached(self) -> bool:
        return self.success_count >= self.min_successful

    def is_failure_tolerance_exceeded(self) -> bool:
        return self._is_failure_condition_reached(
            tolerated_count=self.tolerated_failure_count,
            tolerated_percentage=self.tolerated_failure_percentage,
            failure_count=self.failure_count,
        )

    def _is_failure_condition_reached(
        self,
        tolerated_count: int | None,
        tolerated_percentage: float | None,
        failure_count: int,
    ) -> bool:
        if tolerated_count is not None and failure_count > tolerated_count:
            return True

        if tolerated_percentage is not None and self.total_tasks > 0:
            failure_percentage = (failure_count / self.total_tasks) * 100
            if failure_percentage > tolerated_percentage:
                return True

        return False


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
            await execution_state.create_checkpoint(is_sync=False)
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
                summary_generator=self.summary_generator,
                is_virtual=is_virtual,
            ),
        )
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
            checkpoint = get_checkpoint_result(execution_state, operation_id)

            result: ResultType | None = None
            error = None
            status: BatchItemStatus
            if checkpoint.is_succeeded():
                status = BatchItemStatus.SUCCEEDED
                if checkpoint.is_replay_children():
                    result = await self._execute_item_in_child_context(
                        executor_context, executable
                    )
                elif checkpoint.result is not None:
                    result = await deserialize(
                        serdes=self.item_serdes or self.serdes,
                        data=checkpoint.result,
                        operation_id=operation_id,
                        durable_execution_arn=execution_state.durable_execution_arn,
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

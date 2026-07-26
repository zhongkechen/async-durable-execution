"""Concurrent executor for parallel and map operations."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field as dataclass_field
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, TypeAlias, TypeVar, cast

from ..exceptions import (
    CallableRuntimeError,
    InvalidStateError,
    SuspendExecution,
    TimedSuspendExecution,
    ValidationError,
)
from ..models import (
    ErrorObject,
    Operation,
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
    SerializableModel,
    _metadata,
)
from ..primitive.base import OperationExecutor
from ..primitive.child import (
    ChildOperationExecutor,
    OrphanedChildException,
    _create_child_context_task as _run_in_child_context,
    get_durable_context,
)
from ..context import bind_current_context
from ..execution import durable_callable
from ..serdes import deserialize

if TYPE_CHECKING:
    from ..primitive.child import DurableContext
    from ..serdes import SerDes
    from ..state import ExecutionState


logger = logging.getLogger(__name__)

CallableType = TypeVar("CallableType")
ResultType = TypeVar("ResultType")
R = TypeVar("R")
T = TypeVar("T")
C_contra = TypeVar("C_contra", contravariant=True)

SummaryGenerator: TypeAlias = Callable[[C_contra], str]
"""Create a compact JSON summary for oversized checkpoint payloads."""


class CompletionReason(Enum):
    """Why a `map()` or `parallel()` operation stopped collecting results.

    Values:
        ALL_COMPLETED: Every item or branch reached a terminal state and no
            earlier success, failure, or custom completion condition applied.
        MIN_SUCCESSFUL_REACHED: The configured `min_successful` threshold was
            reached.
        FAILURE_TOLERANCE_EXCEEDED: The number of failures exceeded the
            configured `tolerated_failure_count`, or no failure tolerance was
            configured and at least one failure was observed.
        CUSTOM_COMPLETION_SUCCEEDED: A custom completion function completed the
            operation successfully.
        CUSTOM_COMPLETION_FAILED: A custom completion function completed the
            operation as failed.
    """

    ALL_COMPLETED = "ALL_COMPLETED"
    MIN_SUCCESSFUL_REACHED = "MIN_SUCCESSFUL_REACHED"
    FAILURE_TOLERANCE_EXCEEDED = "FAILURE_TOLERANCE_EXCEEDED"
    CUSTOM_COMPLETION_SUCCEEDED = "CUSTOM_COMPLETION_SUCCEEDED"
    CUSTOM_COMPLETION_FAILED = "CUSTOM_COMPLETION_FAILED"

    @property
    def is_succeeded(self) -> bool:
        """Whether this completion reason represents successful completion."""
        return self in {
            CompletionReason.ALL_COMPLETED,
            CompletionReason.MIN_SUCCESSFUL_REACHED,
            CompletionReason.CUSTOM_COMPLETION_SUCCEEDED,
        }


@dataclass(frozen=True)
class CompletionStatus:
    """Live completion progress passed to a custom completion function.

    Attributes:
        success_count: Number of items or branches that have completed
            successfully.
        failure_count: Number of items or branches that have failed.
        total_count: Total number of items or branches registered for the
            operation.
        completed_count: Calculated number of terminal items or branches,
            equal to `success_count + failure_count`.
        all_completed: Whether every registered item or branch is terminal.

    Raises:
        ValueError: If any count is negative, or if completed count exceeds
            `total_count`.
    """

    success_count: int
    failure_count: int
    total_count: int

    def __post_init__(self) -> None:
        counts = (
            self.success_count,
            self.failure_count,
            self.total_count,
        )
        if any(count < 0 for count in counts):
            msg = "completion counts must be non-negative"
            raise ValueError(msg)
        if self.completed_count > self.total_count:
            msg = "completed_count cannot exceed total_count"
            raise ValueError(msg)

    @property
    def completed_count(self) -> int:
        """Number of items that have reached a terminal state."""
        return self.success_count + self.failure_count

    @property
    def all_completed(self) -> bool:
        """Whether all items have reached a terminal state."""
        return self.completed_count == self.total_count


@dataclass(frozen=True)
class CompletionDecision:
    """Decision returned by a completion condition.

    Args:
        should_complete: Whether the operation should stop collecting results.
        completion_reason: Required when `should_complete` is `True`, and must
            be `None` when `should_complete` is `False`.

    Raises:
        ValueError: If `completion_reason` is missing for a complete decision,
            or present for a continue decision.
    """

    should_complete: bool
    completion_reason: CompletionReason | None = None

    def __post_init__(self) -> None:
        if self.should_complete and self.completion_reason is None:
            msg = "completion_reason is required when should_complete is true"
            raise ValueError(msg)
        if not self.should_complete and self.completion_reason is not None:
            msg = "completion_reason must be None when should_complete is false"
            raise ValueError(msg)

    @staticmethod
    def complete(completion_reason: CompletionReason) -> CompletionDecision:
        """Create a decision that completes the operation.

        Args:
            completion_reason: Reason to store on the resulting `BatchResult`.

        Returns:
            A `CompletionDecision` with `should_complete=True`.
        """
        return CompletionDecision(True, completion_reason)

    @staticmethod
    def continue_execution() -> CompletionDecision:
        """Create a decision that keeps collecting item or branch results.

        Returns:
            A `CompletionDecision` with `should_complete=False`.
        """
        return CompletionDecision(False)

    @property
    def is_succeeded(self) -> bool:
        """Whether this decision completes the operation successfully."""
        return (
            self.should_complete
            and self.completion_reason is not None
            and self.completion_reason.is_succeeded
        )


ShouldComplete: TypeAlias = Callable[[CompletionStatus], CompletionDecision]
"""Callable used by `CompletionConfig.custom()` to decide batch completion."""


class NestingType(Enum):
    """Control how child contexts are created for batch operations."""

    NESTED = "NESTED"
    FLAT = "FLAT"


@dataclass(frozen=True)
class CompletionConfig:
    """Configuration for determining when parallel/map operations complete.

    Without `should_complete`, completion is evaluated in this order:

    1. Complete successfully when `success_count >= min_successful`, if
       `min_successful` is configured.
    2. Complete as failed when `failure_count > tolerated_failure_count`, if
       `tolerated_failure_count` is configured.
    3. Complete as failed when `tolerated_failure_count` is `None` and at least
       one failure is observed.
    4. Complete successfully when every item or branch has completed.

    If `should_complete` is configured, it fully controls the completion
    decision and must return a `CompletionDecision`.

    Args:
        min_successful: Optional success threshold. Reaching this count
            completes the operation successfully.
        tolerated_failure_count: Optional failure tolerance. Failures complete
            the operation as failed only after they exceed this count. When this
            is `None`, any observed failure fails the operation unless the
            success threshold has already been reached.
        should_complete: Optional custom completion function. This is mutually
            exclusive with `min_successful` and `tolerated_failure_count`.

    Raises:
        TypeError: If `should_complete` is provided but is not callable.
        ValueError: If `should_complete` is combined with threshold fields.
    """

    min_successful: int | None = None
    tolerated_failure_count: int | None = None
    should_complete: ShouldComplete | None = None

    def __post_init__(self) -> None:
        if self.should_complete is not None and not callable(self.should_complete):
            msg = "should_complete must be callable"
            raise TypeError(msg)
        if self.should_complete is not None and (
            self.min_successful is not None or self.tolerated_failure_count is not None
        ):
            msg = (
                "should_complete is mutually exclusive with min_successful "
                "and tolerated_failure_count"
            )
            raise ValueError(msg)

    @classmethod
    def thresholds(
        cls,
        *,
        min_successful: int | None = None,
        tolerated_failure_count: int | None = None,
    ) -> CompletionConfig:
        """Create a threshold-based completion configuration.

        Args:
            min_successful: Optional success threshold. The operation completes
                successfully once this many items or branches succeed.
            tolerated_failure_count: Optional failure tolerance. The operation
                completes as failed once failures exceed this count.

        Returns:
            A `CompletionConfig` using the supplied threshold fields.
        """
        return cls(
            min_successful=min_successful,
            tolerated_failure_count=tolerated_failure_count,
        )

    @classmethod
    def first_successful(cls) -> CompletionConfig:
        """Create a configuration that completes after the first success.

        Returns:
            A `CompletionConfig` with `min_successful=1` and no explicit failure
            tolerance. If a failure is observed before any success, the
            operation completes as failed.
        """
        return cls(
            min_successful=1,
            tolerated_failure_count=None,
        )

    @classmethod
    def all_completed(cls) -> CompletionConfig:
        """Create a configuration with no explicit thresholds.

        Returns:
            A `CompletionConfig` with both threshold fields set to `None`. The
            operation completes successfully when all work completes without
            failures, and completes as failed when any failure is observed.
        """
        return cls(
            min_successful=None,
            tolerated_failure_count=None,
        )

    @classmethod
    def all_successful(cls) -> CompletionConfig:
        """Create a configuration that requires every item or branch to succeed.

        Returns:
            A `CompletionConfig` with `tolerated_failure_count=0`. The first
            failure exceeds the zero-failure tolerance and completes the
            operation as failed.
        """
        return cls(
            min_successful=None,
            tolerated_failure_count=0,
        )

    @classmethod
    def custom(cls, should_complete: ShouldComplete) -> CompletionConfig:
        """Create a configuration that delegates completion to a callback.

        Args:
            should_complete: Deterministic callable that receives a
                `CompletionStatus` and returns a `CompletionDecision`.

        Returns:
            A `CompletionConfig` that uses the supplied callback.
        """
        return cls(should_complete=should_complete)

    @property
    def has_custom_should_complete(self) -> bool:
        """Whether a custom completion function is configured."""
        return self.should_complete is not None

    def completion_decision(self, status: CompletionStatus) -> CompletionDecision:
        """Evaluate whether the supplied progress status should complete.

        Args:
            status: Current completion progress for a `map()` or `parallel()`
                operation.

        Returns:
            A `CompletionDecision` describing whether execution should continue
            and, if complete, why.

        Raises:
            TypeError: If a custom completion callback returns `None`.
        """
        if self.should_complete is not None:
            decision = self.should_complete(status)
            if decision is None:
                msg = "should_complete must return a CompletionDecision"
                raise TypeError(msg)
            return decision

        if (
            self.min_successful is not None
            and status.success_count >= self.min_successful
        ):
            return CompletionDecision.complete(CompletionReason.MIN_SUCCESSFUL_REACHED)

        if (
            self.tolerated_failure_count is not None
            and status.failure_count > self.tolerated_failure_count
        ):
            return CompletionDecision.complete(
                CompletionReason.FAILURE_TOLERANCE_EXCEEDED
            )

        if self.tolerated_failure_count is None and status.failure_count > 0:
            return CompletionDecision.complete(
                CompletionReason.FAILURE_TOLERANCE_EXCEEDED
            )

        if status.all_completed:
            return CompletionDecision.complete(CompletionReason.ALL_COMPLETED)

        return CompletionDecision.continue_execution()


def _validate_max_concurrency(max_concurrency: int | None) -> None:
    if max_concurrency is not None and (
        isinstance(max_concurrency, bool)
        or not isinstance(max_concurrency, int)
        or max_concurrency < 1
    ):
        msg = "max_concurrency must be a positive integer or None"
        raise ValidationError(msg)


class BatchItemStatus(Enum):
    """Status of one item or branch inside a batch-style operation."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    STARTED = "STARTED"


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
            if completion_config.has_custom_should_complete:
                status = CompletionStatus(
                    success_count=success_count,
                    failure_count=failure_count,
                    total_count=total_count,
                )
                decision = completion_config.completion_decision(status)
                if decision.should_complete and decision.completion_reason is not None:
                    return decision.completion_reason

                return CompletionReason.ALL_COMPLETED

            has_any_completion_criteria = (
                completion_config.min_successful is not None
                or completion_config.tolerated_failure_count is not None
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
            raise CallableRuntimeError.from_error_object(first_error)

    def get_results(self) -> list[R]:
        return [
            item.result
            for item in self.all
            if item.status is BatchItemStatus.SUCCEEDED and item.result is not None
        ]

    def get_errors(self) -> list[ErrorObject]:
        return [item.error for item in self.failed() if item.error is not None]

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
        self._has_started = False
        self._future: asyncio.Task[ResultType] | None = None
        self._suspend_until: float | None = None
        self._result: ResultType | None = None
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
        return cast("ResultType", self._result)

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
    def has_started(self) -> bool:
        """Whether this branch has acquired a concurrency slot."""
        return self._has_started

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
        self._has_started = True
        self._status = BranchStatus.RUNNING
        self._future = future

    def suspend(self) -> None:
        self._has_started = True
        self._status = BranchStatus.SUSPENDED
        self._suspend_until = None

    def suspend_with_timeout(self, timestamp: float) -> None:
        self._has_started = True
        self._status = BranchStatus.SUSPENDED_WITH_TIMEOUT
        self._suspend_until = timestamp

    def complete(self, result: ResultType) -> None:
        self._has_started = True
        self._status = BranchStatus.COMPLETED
        self._result = result
        self._is_result_set = True

    def fail(self, error: Exception) -> None:
        self._has_started = True
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
        completion_config: CompletionConfig,
    ):
        self.total_tasks = total_tasks
        self.completion_config = completion_config
        self.success_count = 0
        self.failure_count = 0

    def complete_task(self) -> None:
        self.success_count += 1

    def fail_task(self) -> None:
        self.failure_count += 1

    def should_continue(self) -> bool:
        tolerated_failure_count = self.completion_config.tolerated_failure_count

        if tolerated_failure_count is None:
            return self.failure_count == 0

        if (
            tolerated_failure_count is not None
            and self.failure_count > tolerated_failure_count
        ):
            return False

        return True

    def is_complete(self) -> bool:
        completed_count = self.success_count + self.failure_count

        if completed_count == self.total_tasks:
            return True

        min_successful = self.completion_config.min_successful
        return min_successful is not None and self.success_count >= min_successful

    def should_complete(self) -> bool:
        return self.completion_decision().should_complete

    def completion_status(self) -> CompletionStatus:
        return CompletionStatus(
            success_count=self.success_count,
            failure_count=self.failure_count,
            total_count=self.total_tasks,
        )

    def completion_decision(self) -> CompletionDecision:
        if self.completion_config.has_custom_should_complete:
            return self.completion_config.completion_decision(self.completion_status())

        if self.is_complete() or not self.should_continue():
            return CompletionDecision.complete(
                BatchResult._get_completion_reason(
                    failure_count=self.failure_count,
                    success_count=self.success_count,
                    completed_count=self.success_count + self.failure_count,
                    total_count=self.total_tasks,
                    completion_config=self.completion_config,
                )
            )

        return CompletionDecision.continue_execution()

    def is_all_completed(self) -> bool:
        return self.success_count == self.total_tasks

    def is_min_successful_reached(self) -> bool:
        min_successful = self.completion_config.min_successful
        return min_successful is not None and self.success_count >= min_successful

    def is_failure_tolerance_exceeded(self) -> bool:
        return self._is_failure_condition_reached(
            tolerated_count=self.completion_config.tolerated_failure_count,
            failure_count=self.failure_count,
        )

    def _is_failure_condition_reached(
        self,
        tolerated_count: int | None,
        failure_count: int,
    ) -> bool:
        if tolerated_count is not None and failure_count > tolerated_count:
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


class ParallelExecutor(
    OperationExecutor[BatchResult[ResultType]],
    Generic[CallableType, ResultType],
):
    """Execute durable operations concurrently using asyncio tasks."""

    def __init__(
        self,
        execution_state: ExecutionState,
        operation_identifier: OperationIdentifier,
        executor_context: DurableContext,
        executables: list[Executable[CallableType]],
        max_concurrency: int | None,
        completion_config: CompletionConfig,
        top_level_sub_type: OperationSubType,
        iteration_sub_type: OperationSubType,
        name_prefix: str,
        serdes: SerDes | None,
        item_serdes: SerDes | None = None,
        summary_generator: SummaryGenerator | None = None,
        nesting_type: NestingType = NestingType.NESTED,
        branch_namer: Callable[[int], str] | None = None,
    ):
        super().__init__(
            state=execution_state,
            operation_identifier=operation_identifier,
        )
        self.executor_context = executor_context
        self.executables = executables
        self.max_concurrency = max_concurrency
        self.completion_config = completion_config
        self.sub_type_top = top_level_sub_type
        self.sub_type_iteration = iteration_sub_type
        self.name_prefix = name_prefix
        self.summary_generator = summary_generator
        self.nesting_type = nesting_type
        self._branch_namer = branch_namer
        self._completion_event = asyncio.Event()
        self._suspend_exception: SuspendExecution | None = None
        self._completion_exception: Exception | None = None
        self._completion_decision: CompletionDecision | None = None
        self._running_tasks: set[asyncio.Task[ResultType]] = set()

        self.counters = ExecutionCounters(
            len(executables),
            self.completion_config,
        )
        self.executables_with_state: list[ExecutableWithState] = []
        self.serdes = serdes
        self.item_serdes = item_serdes

    async def execute_item(
        self,
        child_context: DurableContext,
        executable: Executable[CallableType],
    ) -> ResultType:
        func = cast("Callable[[], Awaitable[ResultType]]", executable.func)
        with bind_current_context(child_context):
            result: ResultType = await func()
        return result

    def get_iteration_name(self, index: int) -> str:
        if self._branch_namer is not None:
            return self._branch_namer(index)
        return f"{self.name_prefix}{index}"

    async def start(self) -> BatchResult[ResultType]:
        return await self.execute()

    async def replay(self, operation: Operation) -> BatchResult[ResultType]:
        if operation.status is OperationStatus.SUCCEEDED:
            return await self.replay_completed(self.state, self.executor_context)
        return await self.execute()

    async def execute(self) -> BatchResult[ResultType]:
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
        self._completion_exception = None
        self._completion_decision = None
        self._running_tasks.clear()
        next_executable_index = 0

        async def submit_task(
            executable_with_state: ExecutableWithState[CallableType, ResultType],
        ) -> None:
            if self._completion_event.is_set():
                return

            async def run_task() -> ResultType:
                async with semaphore:
                    return await self._execute_item_in_child_context(
                        self.executor_context,
                        executable_with_state.executable,
                    )

            task = asyncio.create_task(run_task())
            executable_with_state.run(task)
            self._running_tasks.add(task)

            def on_done(done_task: asyncio.Task[ResultType]) -> None:
                self._running_tasks.discard(done_task)
                asyncio.create_task(handle_task_completion(done_task))

            async def handle_task_completion(
                done_task: asyncio.Task[ResultType],
            ) -> None:
                await self._on_task_complete(
                    executable_with_state,
                    done_task,
                    scheduler,
                )
                if (
                    not self._completion_event.is_set()
                    and executable_with_state.status
                    in {BranchStatus.COMPLETED, BranchStatus.FAILED}
                ):
                    await submit_next_task()

            task.add_done_callback(on_done)

        async def submit_next_task() -> None:
            nonlocal next_executable_index
            if self._completion_event.is_set() or next_executable_index >= len(
                self.executables_with_state
            ):
                return

            executable_with_state = self.executables_with_state[next_executable_index]
            next_executable_index += 1
            await submit_task(executable_with_state)

        async def resubmitter(
            executable_with_state: ExecutableWithState[CallableType, ResultType],
        ) -> None:
            await self.state.create_checkpoint(is_sync=False)
            await submit_task(executable_with_state)

        async with TimerScheduler(resubmitter) as scheduler:
            for _ in range(min(max_workers, len(self.executables_with_state))):
                await submit_next_task()

            await self._completion_event.wait()

            for task in list(self._running_tasks):
                if not task.done():
                    task.cancel()
            if self._running_tasks:
                await asyncio.gather(*self._running_tasks, return_exceptions=True)

            if self._suspend_exception:
                raise self._suspend_exception
            if self._completion_exception:
                raise self._completion_exception

        return self._create_result()

    def should_execution_suspend(self) -> SuspendResult:
        earliest_timestamp: float = float("inf")
        indefinite_suspend_task: (
            ExecutableWithState[CallableType, ResultType] | None
        ) = None

        for exe_state in self.executables_with_state:
            if exe_state.status is BranchStatus.RUNNING or (
                exe_state.status is BranchStatus.PENDING and exe_state.has_started
            ):
                return SuspendResult.do_not_suspend()
            if exe_state.status is BranchStatus.PENDING:
                continue
            if exe_state.status is BranchStatus.SUSPENDED_WITH_TIMEOUT:
                if (
                    exe_state.suspend_until
                    and exe_state.suspend_until < earliest_timestamp
                ):
                    earliest_timestamp = cast(float, exe_state.suspend_until)
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

        completion_decision = self.counters.completion_decision()
        if completion_decision.should_complete:
            self._completion_decision = completion_decision
            self._completion_event.set()
        else:
            suspend_result = self.should_execution_suspend()
            if suspend_result.should_suspend:
                self._suspend_exception = suspend_result.exception
                self._completion_event.set()
            elif self._all_executables_terminal():
                self._completion_exception = InvalidStateError(
                    "custom should_complete did not complete after all branches "
                    "reached terminal states"
                )
                self._completion_event.set()

    def _all_executables_terminal(self) -> bool:
        return all(
            exe_state.status in {BranchStatus.COMPLETED, BranchStatus.FAILED}
            for exe_state in self.executables_with_state
        )

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
                    BranchStatus.RUNNING
                    | BranchStatus.SUSPENDED
                    | BranchStatus.SUSPENDED_WITH_TIMEOUT
                ):
                    batch_items.append(
                        BatchItem(executable.index, BatchItemStatus.STARTED)
                    )
                case BranchStatus.PENDING:
                    continue

        if (
            self._completion_decision is not None
            and self._completion_decision.completion_reason is not None
        ):
            return BatchResult(
                all=batch_items,
                completion_reason=self._completion_decision.completion_reason,
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

        async def run_child_operation() -> ResultType:
            return await self.execute_item(child_context, executable)

        executor: ChildOperationExecutor[ResultType] = ChildOperationExecutor(
            run_child_operation,
            child_context.execution_state,
            operation_identifier,
            serdes=self.item_serdes or self.serdes,
            summary_generator=self.summary_generator,
            is_virtual=is_virtual,
        )
        return await executor.process()

    async def replay_completed(
        self, execution_state: ExecutionState, executor_context: DurableContext
    ) -> BatchResult[ResultType]:
        items: list[BatchItem[ResultType]] = []
        for executable in self.executables:
            operation_id = (
                executor_context.step_counter._create_step_id_for_logical_step(  # noqa: SLF001
                    executable.index
                )
            )
            operation = execution_state.operations.get(operation_id)

            result: ResultType | None = None
            error = None
            status: BatchItemStatus
            if operation is not None and operation.status is OperationStatus.SUCCEEDED:
                status = BatchItemStatus.SUCCEEDED
                operation_details = operation.context_details
                if operation_details is not None and operation_details.replay_children:
                    result = await self._execute_item_in_child_context(
                        executor_context, executable
                    )
                elif (
                    operation_details is not None
                    and operation_details.result is not None
                ):
                    result = await deserialize(
                        serdes=self.item_serdes or self.serdes,
                        data=operation_details.result,
                        operation_id=operation_id,
                        durable_execution_arn=execution_state.durable_execution_arn,
                        recursive_level=execution_state.recursive_level,
                    )
            elif operation is not None and operation.status is OperationStatus.FAILED:
                error = (
                    operation.context_details.error
                    if operation.context_details is not None
                    else None
                )
                status = BatchItemStatus.FAILED
            else:
                continue

            items.append(
                BatchItem(executable.index, status, result=result, error=error)
            )
        return BatchResult.from_items(items, self.completion_config)


class ParallelSummaryGenerator:
    """Default summary generator for oversized parallel `BatchResult` payloads."""

    def __call__(self, result: BatchResult) -> str:
        fields = {
            "totalCount": result.total_count,
            "successCount": result.success_count,
            "failureCount": result.failure_count,
            "completionReason": result.completion_reason.value,
            "status": result.status.value,
            "startedCount": result.started_count,
            "type": "ParallelResult",
        }

        return json.dumps(fields)


@durable_callable
async def parallel_handler(
    callables: Sequence[Callable[[], Awaitable[R]]],
    execution_state: ExecutionState,
    parallel_context: DurableContext,
    operation_identifier: OperationIdentifier,
    *,
    max_concurrency: int | None = None,
    completion_config: CompletionConfig | None = None,
    serdes: SerDes | None = None,
    item_serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = ParallelSummaryGenerator(),
    nesting_type: NestingType = NestingType.NESTED,
    top_level_sub_type: OperationSubType = OperationSubType.PARALLEL,
    iteration_sub_type: OperationSubType = OperationSubType.PARALLEL_BRANCH,
    name_prefix: str = "parallel-branch-",
    branch_namer: Callable[[int], str] | None = None,
):
    """Execute multiple operations in parallel."""
    # Summary Generator Construction (matches TypeScript implementation):
    # Construct the summary generator at the handler level, just like TypeScript does in parallel-handler.ts.
    # This matches the pattern where handlers are responsible for configuring operation-specific behavior.
    #
    # See TypeScript reference: aws-durable-execution-sdk-js/src/handlers/parallel-handler/parallel-handler.ts (~line 112)

    executor: ParallelExecutor[Callable[[], Awaitable[R]], R] = ParallelExecutor(
        executables=[
            Executable(index=i, func=func) for i, func in enumerate(callables)
        ],
        max_concurrency=max_concurrency,
        completion_config=completion_config or CompletionConfig.all_successful(),
        top_level_sub_type=top_level_sub_type,
        iteration_sub_type=iteration_sub_type,
        name_prefix=name_prefix,
        serdes=serdes,
        summary_generator=summary_generator,
        item_serdes=item_serdes,
        nesting_type=nesting_type,
        branch_namer=branch_namer,
        execution_state=execution_state,
        operation_identifier=operation_identifier,
        executor_context=parallel_context,
    )

    return await executor.process()


def parallel(
    branches: Iterable[Callable[[], Awaitable[T]]],
    *,
    name: str | None = None,
    max_concurrency: int | None = None,
    completion_config: CompletionConfig | None = None,
    serdes: SerDes | None = None,
    item_serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = ParallelSummaryGenerator(),
    nesting_type: NestingType = NestingType.NESTED,
) -> asyncio.Task[BatchResult[T]]:
    """Start a durable parallel operation.

    Each branch is an async zero-argument callable, typically a bound durable
    callable such as `fetch_user(user_id)`. Branches run in child durable
    contexts and may contain durable operations such as `step()` or `wait()`.

    The returned object is an `asyncio.Task`; awaiting it yields a `BatchResult`.
    Calling `parallel()` without immediately awaiting it schedules the durable
    operation in the background, consistent with other operation helpers.

    By default, `parallel()` uses `CompletionConfig.all_successful()`: every
    branch must succeed, and the first failure completes the operation as failed.
    Pass `completion_config` to use threshold-based or custom completion.

    Args:
        branches: Async zero-argument branch callables to run concurrently.
        name: Optional durable operation name.
        max_concurrency: Optional limit for in-flight branches. A suspended
            branch retains its slot until it reaches a terminal state.
        completion_config: Optional completion policy. Use
            `CompletionConfig.thresholds()`, `first_successful()`,
            `all_completed()`, `all_successful()`, or `custom()`.
        serdes: Optional serializer for the final `BatchResult`.
        item_serdes: Optional serializer for each branch result.
        summary_generator: Optional callable used to summarize oversized
            checkpoint payloads.
        nesting_type: Whether branch operations use nested or flat operation
            identifiers.

    Returns:
        An `asyncio.Task` that resolves to a `BatchResult` containing one
        `BatchItem` per branch.

    Raises:
        ValidationError: If `max_concurrency` is not a positive integer or
            `None`.
        RuntimeError: If called outside a durable context.
    """
    _validate_max_concurrency(max_concurrency)
    context = get_durable_context("parallel")
    validated_branches: list[Callable[[], Awaitable[T]]] = []
    for branch in branches:
        validated_branches.append(branch)

    async def run_parallel_handler() -> BatchResult[T]:
        parallel_context = get_durable_context("parallel")
        operation_id = parallel_context.step_id_prefix
        if operation_id is None:
            msg = "parallel operation id is not available in the current context"
            raise RuntimeError(msg)
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.PARALLEL,
            parent_id=parallel_context.parent_id,
            name=name,
        )

        handler = parallel_handler(
            callables=validated_branches,
            execution_state=context.execution_state,
            parallel_context=parallel_context,
            operation_identifier=operation_identifier,
            max_concurrency=max_concurrency,
            completion_config=completion_config or CompletionConfig.all_successful(),
            serdes=serdes,
            item_serdes=item_serdes,
            summary_generator=summary_generator,
            nesting_type=nesting_type,
        )
        return await handler()

    return _run_in_child_context(
        run_parallel_handler,
        sub_type=OperationSubType.PARALLEL,
        name=name,
        serdes=serdes,
        operation_name="parallel",
    )

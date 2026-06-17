"""Configuration types."""

from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Generic, TypeVar

from .exceptions import ValidationError
from .models import (
    RetryDecision,
    WaitDecision,
    WaitForConditionDecision,
)


P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type
T = TypeVar("T")
U = TypeVar("U")

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from concurrent.futures import Future

    from .models import OperationSubType
    from .serdes import SerDes
    from .types import SummaryGenerator


Numeric = int | float  # deliberately leaving off complex


def duration_to_seconds(duration: timedelta, field_name: str = "duration") -> int:
    """Convert a timedelta to whole seconds after validating it is non-negative."""
    total_seconds = duration.total_seconds()
    if total_seconds < 0:
        msg = f"{field_name} must be non-negative"
        raise ValidationError(msg)
    return int(total_seconds)


@dataclass(frozen=True)
class BatchedInput(Generic[T, U]):
    batch_input: T
    items: list[U]


class TerminationMode(Enum):
    TERMINATE = "TERMINATE"
    CANCEL = "CANCEL"
    WAIT = "WAIT"
    ABANDON = "ABANDON"


class NestingType(Enum):
    """Control how child contexts are created for batch operations.

    Applies to `map` and `parallel`. Each branch or iteration runs inside a
    child context.

        - NESTED: full checkpointed context
        - FLAT: a virtual context that skips checkpoints for the branch/iteration.

    """

    NESTED = "NESTED"
    """Create CONTEXT operations for each branch/iteration with full checkpointing.

    Operations within each branch/iteration are wrapped in their own context.

    - Observability: high — each branch/iteration appears as a separate
      operation in execution history.
    - Cost: higher — consumes more operations due to CONTEXT creation
      overhead.
    - Scale: lower maximum iterations due to operation limits.
    """

    FLAT = "FLAT"
    """Skip CONTEXT operations for branches/iterations using virtual contexts.

    Operations execute directly without individual context wrapping.

    - Observability: lower — branches/iterations don't appear as separate
      operations in execution history.
    - Cost: ~30% lower — reduces operation consumption by skipping CONTEXT
      overhead.
    - Scale: higher maximum iterations possible within operation limits.
    """


@dataclass(frozen=True)
class CompletionConfig:
    """Configuration for determining when parallel/map operations complete.

    This class defines the success/failure criteria for operations that process
    multiple items or branches concurrently.

    Args:
        min_successful: Minimum number of successful completions required.
            If None, no minimum is enforced. Use this to implement "at least N
            must succeed" semantics.

        tolerated_failure_count: Maximum number of failures allowed before
            the operation is considered failed. If None, no limit on failure count.
            Use this to implement "fail fast after N failures" semantics.

        tolerated_failure_percentage: Maximum percentage of failures allowed
            (0.0 to 100.0). If None, no percentage limit is enforced.
            Use this to implement "fail if more than X% fail" semantics.

    Note:
        The operation completes when any of the completion criteria are met:
        - Enough successes (min_successful reached)
        - Too many failures (tolerated limits exceeded)
        - All items/branches completed

    Example:
        # Succeed if at least 3 succeed, fail if more than 2 fail
        config = CompletionConfig(
            min_successful=3,
            tolerated_failure_count=2
        )
    """

    min_successful: int | None = None
    tolerated_failure_count: int | None = None
    tolerated_failure_percentage: int | float | None = None

    # TODO: reevaluate this
    # @staticmethod
    # def first_completed():
    #     return CompletionConfig(
    #         min_successful=None, tolerated_failure_count=None, tolerated_failure_percentage=None
    #     )

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


@dataclass(frozen=True)
class ParallelConfig:
    """Configuration options for parallel execution operations.

    This class configures how parallel operations are executed, including
    concurrency limits, completion criteria, and serialization behavior.

    Args:
        max_concurrency: Maximum number of parallel branches to execute concurrently.
            If None, no limit is imposed and all branches run concurrently.
            Use this to control resource usage and prevent overwhelming the system.

        completion_config: Defines when the parallel operation should complete.
            Controls success/failure criteria for the overall parallel operation.
            Default is CompletionConfig.all_successful() which requires all branches
            to succeed. Other options include first_successful() and all_completed().

        serdes: Custom serialization/deserialization configuration for BatchResult.
            Applied at the handler level to serialize the entire BatchResult object.
            If None, uses the default JSON serializer for BatchResult.

            Backward Compatibility: If only 'serdes' is provided (no item_serdes),
            it will be used for both individual functions AND BatchResult serialization
            to maintain existing behavior.

        item_serdes: Custom serialization/deserialization configuration for individual functions.
            Applied to each function's result as tasks complete in child contexts.
            If None, uses the default JSON serializer for individual function results.

            When both 'serdes' and 'item_serdes' are provided:
            - item_serdes: Used for individual function results in child contexts
            - serdes: Used for the entire BatchResult at handler level

        summary_generator: Function to generate compact summaries for large results (>256KB).
            When the serialized result exceeds CHECKPOINT_SIZE_LIMIT, this generator
            creates a JSON summary instead of checkpointing the full result. The operation
            is marked with ReplayChildren=true to reconstruct the full result during replay.

            Used internally by map/parallel operations to handle large BatchResult payloads.
            Signature: (result: T) -> str

        nesting_type: How child operations should inherit context from their parent.
            - NESTED: Each branch runs in its own isolated context (default)
            - FLAT: All branches share the same parent context

    Example:
        # Run at most 3 branches concurrently, succeed if any one succeeds
        config = ParallelConfig(
            max_concurrency=3,
            completion_config=CompletionConfig.first_successful()
        )
    """

    max_concurrency: int | None = None
    completion_config: CompletionConfig = field(
        default_factory=CompletionConfig.all_successful
    )
    serdes: SerDes | None = None
    item_serdes: SerDes | None = None
    summary_generator: SummaryGenerator | None = None
    nesting_type: NestingType = NestingType.NESTED


@dataclass(frozen=True)
class ParallelBranch(Generic[T]):
    """A named branch for parallel execution.

    Use this to provide custom names for parallel branches, improving
    observability in execution history.

    Type Parameters:
        T: The return type of the branch function.

    Args:
        func: The callable to execute in this branch. Receives a DurableContext.
        name: Optional custom name for this branch. When provided, replaces
            the default "parallel-branch-{index}" naming in execution history.
            This affects observability but not replay determinism.

    Example:
        async def fetch_user(ctx: DurableContext) -> dict:
            ...

        async def fetch_orders(ctx: DurableContext) -> dict:
            ...

        await parallel(
            functions=[
                ParallelBranch(func=fetch_user, name="fetch-user-data"),
                ParallelBranch(func=fetch_orders, name="fetch-order-history"),
            ],
            name="load-data",
            config=ParallelConfig(max_concurrency=2),
        )
    """

    func: Callable[..., Awaitable[T]]
    name: str | None = None

    async def __call__(self, *args, **kwargs) -> T:
        """Delegate to the wrapped function, making ParallelBranch itself callable."""
        return await self.func(*args, **kwargs)


class StepSemantics(Enum):
    AT_MOST_ONCE_PER_RETRY = "AT_MOST_ONCE_PER_RETRY"
    AT_LEAST_ONCE_PER_RETRY = "AT_LEAST_ONCE_PER_RETRY"


@dataclass(frozen=True)
class StepConfig:
    """Configuration for a step."""

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY
    serdes: SerDes | None = None


@dataclass(frozen=True)
class ChildConfig(Generic[T]):
    """Configuration options for child context operations.

    This class configures how child contexts are executed and checkpointed,
    matching the TypeScript ChildConfig interface behavior.

    Args:
        serdes: Custom serialization/deserialization configuration for BatchResult.
            Applied at the handler level to serialize the entire BatchResult object.
            If None, uses the default JSON serializer for BatchResult.

            Backward Compatibility: If only 'serdes' is provided (no item_serdes),
            it will be used for both individual items AND BatchResult serialization
            to maintain existing behavior.

        item_serdes: Custom serialization/deserialization configuration for individual items.
            Applied to each item's result as tasks complete in child contexts.
            If None, uses the default JSON serializer for individual items.

            When both 'serdes' and 'item_serdes' are provided:
            - item_serdes: Used for individual item results in child contexts
            - serdes: Used for the entire BatchResult at handler level

        sub_type: Operation subtype identifier used for tracking and debugging.
            Examples: OperationSubType.MAP_ITERATION, OperationSubType.PARALLEL_BRANCH.
            Used internally by the execution engine for operation classification.

        summary_generator: Function to generate compact summaries for large results (>256KB).
            When the serialized result exceeds CHECKPOINT_SIZE_LIMIT, this generator
            creates a JSON summary instead of checkpointing the full result. The operation
            is marked with ReplayChildren=true to reconstruct the full result during replay.

            Used internally by map/parallel operations to handle large BatchResult payloads.
            Signature: (result: T) -> str

        is_virtual: When True, skip all checkpoints (START, SUCCEED,
            FAIL) for this child context and propagate the caller's reporting
            parent id through to operations created inside the child. The
            branch is a logical scope for step-id prefixing but does not
            appear in the execution history. Used internally by
            NestingType.FLAT branches. Use this to group operations without
            adding a CONTEXT entry to the execution history.

    See TypeScript reference: aws-durable-execution-sdk-js/src/types/index.ts
    """

    serdes: SerDes | None = None
    item_serdes: SerDes | None = None
    sub_type: OperationSubType | None = None
    summary_generator: SummaryGenerator | None = None
    is_virtual: bool = False


@dataclass(frozen=True)
class WithRetryConfig(Generic[T]):
    """Configuration for with_retry.

    Holds a retry strategy callable (same type used by StepConfig) and
    adds execution-mode options specific to with_retry.

    Attributes:
        retry_strategy: A callable that decides whether to retry and with
            what delay. Accepts (Exception, int) and returns RetryDecision.
            Use RetryStrategyBuilder(...).build() to build one, or provide
            a custom callable. If None, the default retry strategy
            (RetryStrategyBuilder defaults) is used.
        wrap_with_run_in_child_context: Whether to wrap the retry loop in
            a child context for isolation. Default True. When True, final
            failure is rethrown as CallableRuntimeError with the original
            exception on `cause`. When False, the original error is
            rethrown unchanged.
        child_context_config: Optional ChildConfig forwarded to
            run_in_child_context when wrapping is enabled. Ignored when
            wrap_with_run_in_child_context is False.
    """

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    wrap_with_run_in_child_context: bool = True
    child_context_config: ChildConfig[T] | None = None


class ItemsPerBatchUnit(Enum):
    COUNT = ("COUNT",)
    BYTES = "BYTES"


@dataclass(frozen=True)
class ItemBatcher(Generic[T]):
    """Configuration for batching items in map operations.

    This class defines how individual items should be grouped together into batches
    for more efficient processing in map operations.

    Args:
        max_items_per_batch: Maximum number of items to include in a single batch.
            If 0 (default), no item count limit is applied. Use this to control
            batch size when processing many small items.

        max_item_bytes_per_batch: Maximum total size in bytes for items in a batch.
            If 0 (default), no size limit is applied. Use this to control memory
            usage when processing large items or when items vary significantly in size.

        batch_input: Additional data to include with each batch.
            This data is passed to the processing function along with the batched items.
            Useful for providing context or configuration that applies to all items
            in the batch.

    Example:
        # Batch up to 100 items or 1MB, whichever comes first
        batcher = ItemBatcher(
            max_items_per_batch=100,
            max_item_bytes_per_batch=1024*1024,
            batch_input={"processing_mode": "fast"}
        )
    """

    max_items_per_batch: int = 0
    max_item_bytes_per_batch: int | float = 0
    batch_input: T | None = None


@dataclass(frozen=True)
class MapConfig(Generic[T]):
    """Configuration options for map operations over collections.

    This class configures how map operations process collections of items,
    including concurrency, batching, completion criteria, and serialization.

    Type Parameters:
        T: The type of items being processed in the map operation.

    Args:
        max_concurrency: Maximum number of items to process concurrently.
            If None, no limit is imposed and all items are processed concurrently.
            Use this to control resource usage when processing large collections.

        item_batcher: Configuration for batching multiple items together for processing.
            Allows grouping items by count or size to optimize processing efficiency.
            Default is no batching (each item processed individually).

        completion_config: Defines when the map operation should complete.
            Controls success/failure criteria for the overall map operation.
            Default allows any number of failures. Use CompletionConfig.all_successful()
            to require all items to succeed.

        serdes: Custom serialization/deserialization configuration for BatchResult.
            Applied at the handler level to serialize the entire BatchResult object.
            If None, uses the default JSON serializer for BatchResult.

            Backward Compatibility: If only 'serdes' is provided (no item_serdes),
            it will be used for both individual items AND BatchResult serialization
            to maintain existing behavior.

        item_serdes: Custom serialization/deserialization configuration for individual items.
            Applied to each item's result as tasks complete in child contexts.
            If None, uses the default JSON serializer for individual items.

            When both 'serdes' and 'item_serdes' are provided:
            - item_serdes: Used for individual item results in child contexts
            - serdes: Used for the entire BatchResult at handler level

        summary_generator: Function to generate compact summaries for large results (>256KB).
            When the serialized result exceeds CHECKPOINT_SIZE_LIMIT, this generator
            creates a JSON summary instead of checkpointing the full result. The operation
            is marked with ReplayChildren=true to reconstruct the full result during replay.

            Used internally by map/parallel operations to handle large BatchResult payloads.
            Signature: (result: T) -> str

        nesting_type: How child operations should inherit context from their parent.
            - NESTED: Each item runs in its own isolated context (default)
            - FLAT: All items share the same parent context

        item_namer: Optional callable to generate custom names for each map iteration.
            When provided, replaces the default "map-item-{index}" naming scheme.
            Receives the item and its index, and returns a string name for that iteration.
            This affects observability (execution history names) but not replay determinism.
            If None, uses the default naming: "map-item-{index}".

    Example:
        # Process 5 items at a time, batch by count, require all to succeed
        config = MapConfig(
            max_concurrency=5,
            item_batcher=ItemBatcher(max_items_per_batch=10),
            completion_config=CompletionConfig.all_successful()
        )

        # With custom iteration names
        config = MapConfig(
            max_concurrency=5,
            item_namer=lambda item, index: f"process-order-{item.id}"
        )
    """

    max_concurrency: int | None = None
    item_batcher: ItemBatcher = field(default_factory=ItemBatcher)
    completion_config: CompletionConfig = field(default_factory=CompletionConfig)
    serdes: SerDes | None = None
    item_serdes: SerDes | None = None
    summary_generator: SummaryGenerator | None = None
    nesting_type: NestingType = NestingType.NESTED
    item_namer: Callable[[T, int], str] | None = None


@dataclass(frozen=True)
class InvokeConfig(Generic[P, R]):
    """
    Configuration for invoke operations.

    This class configures how function invocations are executed, including
    timeout behavior, serialization, and tenant isolation.

    Args:
        serdes_payload: Custom serialization/deserialization for the payload
            sent to the invoked function. Defaults to DEFAULT_JSON_SERDES when
            not set.

        serdes_result: Custom serialization/deserialization for the result
            returned from the invoked function. Defaults to DEFAULT_JSON_SERDES when
            not set.

        tenant_id: Optional tenant identifier for multi-tenant isolation.
            If provided, the invocation will be scoped to this tenant.
    """

    # retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    serdes_payload: SerDes[P] | None = None
    serdes_result: SerDes[R] | None = None
    tenant_id: str | None = None


@dataclass(frozen=True)
class CallbackConfig:
    """Configuration for callbacks."""

    timeout: timedelta = field(default_factory=timedelta)
    heartbeat_timeout: timedelta = field(default_factory=timedelta)
    serdes: SerDes | None = None

    def __post_init__(self):
        duration_to_seconds(self.timeout, "timeout")
        duration_to_seconds(self.heartbeat_timeout, "heartbeat_timeout")

    @property
    def timeout_seconds(self) -> int:
        """Get timeout in seconds."""
        return duration_to_seconds(self.timeout, "timeout")

    @property
    def heartbeat_timeout_seconds(self) -> int:
        """Get heartbeat timeout in seconds."""
        return duration_to_seconds(self.heartbeat_timeout, "heartbeat_timeout")


@dataclass(frozen=True)
class WaitForCallbackConfig(CallbackConfig):
    """Configuration for wait for callback."""

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None


@dataclass(frozen=True)
class WaitForConditionConfig(Generic[T]):
    """Configuration for wait_for_condition."""

    wait_strategy: Callable[[T, int], WaitForConditionDecision]
    initial_state: T
    serdes: SerDes | None = None


class StepFuture(Generic[T]):
    """A future that will block on result() until the step returns."""

    def __init__(self, future: Future[T], name: str | None = None):
        self.name = name
        self.future = future

    def result(self, timeout_seconds: int | None = None) -> T:
        """Return the result of the Future."""
        return self.future.result(timeout=timeout_seconds)


class JitterStrategy(StrEnum):
    """
    Jitter strategies are used to introduce noise when attempting to retry
    an invoke. We introduce noise to prevent a thundering-herd effect where
    a group of accesses (e.g. invokes) happen at once.

    Jitter is meant to be used to spread operations across time.

    Based on AWS Architecture Blog: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    members:
        :NONE: No jitter; use the exact calculated delay
        :FULL: Full jitter; random delay between 0 and calculated delay
        :HALF: Equal jitter; random delay between 0.5x and 1.0x of the calculated delay
    """

    NONE = "NONE"
    FULL = "FULL"
    HALF = "HALF"

    def apply_jitter(self, delay: float) -> float:
        """Apply jitter to a delay value and return the final delay.

        Args:
            delay: The base delay value to apply jitter to

        Returns:
            The final delay after applying jitter strategy
        """
        match self:
            case JitterStrategy.NONE:
                return delay
            case JitterStrategy.HALF:
                # Equal jitter: delay/2 + random(0, delay/2)
                return delay / 2 + random.random() * (delay / 2)  # noqa: S311
            case _:  # default is FULL
                # Full jitter: random(0, delay)
                return random.random() * delay  # noqa: S311


@dataclass
class WaitStrategyBuilder(Generic[T]):
    should_continue_polling: Callable[[T], bool]
    max_attempts: int = 60
    initial_delay: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    max_delay: timedelta = field(
        default_factory=lambda: timedelta(minutes=5)
    )  # 5 minutes
    backoff_rate: Numeric = 1.5
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    timeout: timedelta | None = None  # Not implemented yet

    def __post_init__(self):
        duration_to_seconds(self.initial_delay, "initial_delay")
        duration_to_seconds(self.max_delay, "max_delay")
        if self.timeout is not None:
            duration_to_seconds(self.timeout, "timeout")

    @property
    def initial_delay_seconds(self) -> int:
        """Get initial delay in seconds."""
        return duration_to_seconds(self.initial_delay, "initial_delay")

    @property
    def max_delay_seconds(self) -> int:
        """Get max delay in seconds."""
        return duration_to_seconds(self.max_delay, "max_delay")

    @property
    def timeout_seconds(self) -> int | None:
        """Get timeout in seconds."""
        if self.timeout is None:
            return None
        return duration_to_seconds(self.timeout, "timeout")

    def build(self) -> Callable[[T, int], WaitDecision]:
        """Build a wait strategy callable from this builder."""

        def wait_strategy(result: T, attempts_made: int) -> WaitDecision:
            if not self.should_continue_polling(result):
                return WaitDecision.no_wait()

            if attempts_made >= self.max_attempts:
                return WaitDecision.no_wait()

            base_delay: float = min(
                self.initial_delay_seconds * (self.backoff_rate ** (attempts_made - 1)),
                self.max_delay_seconds,
            )
            delay_with_jitter: float = self.jitter_strategy.apply_jitter(base_delay)
            final_delay: int = max(1, math.ceil(delay_with_jitter))

            return WaitDecision.wait(timedelta(seconds=final_delay))

        return wait_strategy


@dataclass
class RetryStrategyBuilder:
    max_attempts: int = 3
    initial_delay: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    max_delay: timedelta = field(
        default_factory=lambda: timedelta(minutes=5)
    )  # 5 minutes
    backoff_rate: Numeric = 2.0
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    retryable_errors: list[str | re.Pattern] | None = None
    retryable_error_types: list[type[Exception]] | None = None

    def __post_init__(self):
        duration_to_seconds(self.initial_delay, "initial_delay")
        duration_to_seconds(self.max_delay, "max_delay")

    @property
    def initial_delay_seconds(self) -> int:
        """Get initial delay in seconds."""
        return duration_to_seconds(self.initial_delay, "initial_delay")

    @property
    def max_delay_seconds(self) -> int:
        """Get max delay in seconds."""
        return duration_to_seconds(self.max_delay, "max_delay")

    def build(self) -> Callable[[Exception, int], RetryDecision]:
        """Build a retry strategy callable from this builder."""
        default_retryable_error_pattern = re.compile(r".*")
        should_use_default_errors: bool = (
            self.retryable_errors is None and self.retryable_error_types is None
        )

        retryable_errors: list[str | re.Pattern] = (
            self.retryable_errors
            if self.retryable_errors is not None
            else (
                [default_retryable_error_pattern] if should_use_default_errors else []
            )
        )
        retryable_error_types: list[type[Exception]] = self.retryable_error_types or []

        def retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
            if attempts_made >= self.max_attempts:
                return RetryDecision.no_retry()

            is_retryable_error_message: bool = any(
                pattern.search(str(error))
                if isinstance(pattern, re.Pattern)
                else pattern in str(error)
                for pattern in retryable_errors
            )
            is_retryable_error_type: bool = any(
                isinstance(error, error_type) for error_type in retryable_error_types
            )

            if not is_retryable_error_message and not is_retryable_error_type:
                return RetryDecision.no_retry()

            base_delay: float = min(
                self.initial_delay_seconds * (self.backoff_rate ** (attempts_made - 1)),
                self.max_delay_seconds,
            )
            delay_with_jitter: float = self.jitter_strategy.apply_jitter(base_delay)
            final_delay: int = max(1, math.ceil(delay_with_jitter))

            return RetryDecision.retry(timedelta(seconds=final_delay))

        return retry_strategy


class RetryPresets:
    """Default retry presets."""

    @classmethod
    def none(cls) -> Callable[[Exception, int], RetryDecision]:
        """No retries."""
        return RetryStrategyBuilder(max_attempts=1).build()

    @classmethod
    def default(cls) -> Callable[[Exception, int], RetryDecision]:
        """Default retries, will be used automatically if retryConfig is missing."""
        return RetryStrategyBuilder(
            max_attempts=6,
            initial_delay=timedelta(seconds=5),
            max_delay=timedelta(minutes=1),
            backoff_rate=2,
            jitter_strategy=JitterStrategy.FULL,
        ).build()

    @classmethod
    def transient(cls) -> Callable[[Exception, int], RetryDecision]:
        """Quick retries for transient errors."""
        return RetryStrategyBuilder(
            max_attempts=3, backoff_rate=2, jitter_strategy=JitterStrategy.HALF
        ).build()

    @classmethod
    def resource_availability(cls) -> Callable[[Exception, int], RetryDecision]:
        """Longer retries for resource availability."""
        return RetryStrategyBuilder(
            max_attempts=5,
            initial_delay=timedelta(seconds=5),
            max_delay=timedelta(minutes=5),
            backoff_rate=2,
        ).build()

    @classmethod
    def critical(cls) -> Callable[[Exception, int], RetryDecision]:
        """Aggressive retries for critical operations."""
        return RetryStrategyBuilder(
            max_attempts=10,
            initial_delay=timedelta(seconds=1),
            max_delay=timedelta(minutes=1),
            backoff_rate=1.5,
            jitter_strategy=JitterStrategy.NONE,
        ).build()

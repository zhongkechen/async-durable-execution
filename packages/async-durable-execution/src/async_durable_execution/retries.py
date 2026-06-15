"""Ready-made retry strategies and retry creators."""

from __future__ import annotations
import math
import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

from async_durable_execution.config import (
    JitterStrategy,
    duration_to_seconds,
)


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from async_durable_execution.config import ChildConfig
    from async_durable_execution.types import DurableContext

T = TypeVar("T")

Numeric = int | float

# Default pattern that matches all error messages
_DEFAULT_RETRYABLE_ERROR_PATTERN = re.compile(r".*")


@dataclass
class RetryDecision:
    """Decision about whether to retry a step and with what delay."""

    should_retry: bool
    delay: timedelta

    def __post_init__(self):
        duration_to_seconds(self.delay)

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return duration_to_seconds(self.delay)

    @classmethod
    def retry(cls, delay: timedelta) -> RetryDecision:
        """Create a retry decision."""
        return cls(should_retry=True, delay=delay)

    @classmethod
    def no_retry(cls) -> RetryDecision:
        """Create a no-retry decision."""
        return cls(should_retry=False, delay=timedelta())


@dataclass
class RetryStrategyConfig:
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


def create_retry_strategy(
    config: RetryStrategyConfig | None = None,
) -> Callable[[Exception, int], RetryDecision]:
    if config is None:
        config = RetryStrategyConfig()

    # Apply default retryableErrors only if user didn't specify either filter
    should_use_default_errors: bool = (
        config.retryable_errors is None and config.retryable_error_types is None
    )

    retryable_errors: list[str | re.Pattern] = (
        config.retryable_errors
        if config.retryable_errors is not None
        else ([_DEFAULT_RETRYABLE_ERROR_PATTERN] if should_use_default_errors else [])
    )
    retryable_error_types: list[type[Exception]] = config.retryable_error_types or []

    def retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
        # Check if we've exceeded max attempts
        if attempts_made >= config.max_attempts:
            return RetryDecision.no_retry()

        # Check if error is retryable based on error message
        is_retryable_error_message: bool = any(
            pattern.search(str(error))
            if isinstance(pattern, re.Pattern)
            else pattern in str(error)
            for pattern in retryable_errors
        )

        # Check if error is retryable based on error type
        is_retryable_error_type: bool = any(
            isinstance(error, error_type) for error_type in retryable_error_types
        )

        if not is_retryable_error_message and not is_retryable_error_type:
            return RetryDecision.no_retry()

        # Calculate delay with exponential backoff
        base_delay: float = min(
            config.initial_delay_seconds * (config.backoff_rate ** (attempts_made - 1)),
            config.max_delay_seconds,
        )
        # Apply jitter to get final delay
        delay_with_jitter: float = config.jitter_strategy.apply_jitter(base_delay)
        # Round up and ensure minimum of 1 second
        final_delay: int = max(1, math.ceil(delay_with_jitter))

        return RetryDecision.retry(timedelta(seconds=final_delay))

    return retry_strategy


class RetryPresets:
    """Default retry presets."""

    @classmethod
    def none(cls) -> Callable[[Exception, int], RetryDecision]:
        """No retries."""
        return create_retry_strategy(RetryStrategyConfig(max_attempts=1))

    @classmethod
    def default(cls) -> Callable[[Exception, int], RetryDecision]:
        """Default retries, will be used automatically if retryConfig is missing"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=6,
                initial_delay=timedelta(seconds=5),
                max_delay=timedelta(minutes=1),
                backoff_rate=2,
                jitter_strategy=JitterStrategy.FULL,
            )
        )

    @classmethod
    def transient(cls) -> Callable[[Exception, int], RetryDecision]:
        """Quick retries for transient errors"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=3, backoff_rate=2, jitter_strategy=JitterStrategy.HALF
            )
        )

    @classmethod
    def resource_availability(cls) -> Callable[[Exception, int], RetryDecision]:
        """Longer retries for resource availability"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=5,
                initial_delay=timedelta(seconds=5),
                max_delay=timedelta(minutes=5),
                backoff_rate=2,
            )
        )

    @classmethod
    def critical(cls) -> Callable[[Exception, int], RetryDecision]:
        """Aggressive retries for critical operations"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=10,
                initial_delay=timedelta(seconds=1),
                max_delay=timedelta(minutes=1),
                backoff_rate=1.5,
                jitter_strategy=JitterStrategy.NONE,
            )
        )


@dataclass(frozen=True)
class WithRetryConfig(Generic[T]):
    """Configuration for with_retry.

    Holds a retry strategy callable (same type used by StepConfig) and
    adds execution-mode options specific to with_retry.

    Attributes:
        retry_strategy: A callable that decides whether to retry and with
            what delay. Accepts (Exception, int) and returns RetryDecision.
            Use create_retry_strategy(RetryStrategyConfig(...)) to build one,
            or provide a custom callable. If None, the default retry strategy
            (RetryStrategyConfig defaults) is used.
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


@overload
async def with_retry(
    context: DurableContext,
    func: Callable[[int], Awaitable[T]],
    config: WithRetryConfig[T],
    name: str | None = None,
) -> T: ...


@overload
async def with_retry(
    context: Callable[[int], Awaitable[T]],
    func: WithRetryConfig[T],
    config: None = None,
    name: str | None = None,
) -> T: ...


async def with_retry(
    context: DurableContext | Callable[[int], Awaitable[T]],
    func: Callable[[int], Awaitable[T]] | WithRetryConfig[T],
    config: WithRetryConfig[T] | None = None,
    name: str | None = None,
) -> T:
    """Compatibility wrapper for the context.with_retry implementation."""
    from async_durable_execution import context as context_module

    context_with_retry_impl = cast(Any, getattr(context_module, "with_retry"))

    if config is None:
        return await context_with_retry_impl(
            cast("Callable[[int], Awaitable[T]]", context),
            cast("WithRetryConfig[T]", func),
            None,
            name,
        )

    return await context_with_retry_impl(
        cast("DurableContext", context),
        cast("Callable[[int], Awaitable[T]]", func),
        config,
        name,
    )

"""Configuration types."""

from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, TypeAlias

from .exceptions import ValidationError

if TYPE_CHECKING:
    from collections.abc import Callable

Duration: TypeAlias = int | timedelta


def duration_to_seconds(duration: Duration, field_name: str = "duration") -> int:
    """Convert a seconds integer or timedelta to whole seconds."""
    if isinstance(duration, bool) or not isinstance(duration, int | timedelta):
        msg = f"{field_name} must be an int number of seconds or a timedelta"
        raise ValidationError(msg)

    seconds = (
        int(duration.total_seconds()) if isinstance(duration, timedelta) else duration
    )
    if seconds < 0:
        msg = f"{field_name} must be non-negative"
        raise ValidationError(msg)
    return seconds


@dataclass(frozen=True)
class RetryDecision:
    """Decision about whether to retry an operation and with what delay."""

    should_retry: bool
    delay: Duration

    def __post_init__(self):
        object.__setattr__(self, "delay", duration_to_seconds(self.delay, "delay"))

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return duration_to_seconds(self.delay, "delay")

    @classmethod
    def retry(cls, delay: Duration) -> "RetryDecision":
        """Create a retry decision."""
        return cls(should_retry=True, delay=delay)

    @classmethod
    def retry_after_delay(cls, delay_seconds: int) -> "RetryDecision":
        """Create a retry decision from a delay in seconds."""
        return cls.retry(delay_seconds)

    @classmethod
    def no_retry(cls) -> "RetryDecision":
        """Create a no-retry decision."""
        return cls(should_retry=False, delay=0)


class JitterStrategy(str, Enum):
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
class RetryStrategyBuilder:
    """Build exponential-backoff retry strategies for durable operations."""

    max_attempts: int = 3
    initial_delay: Duration = 5
    max_delay: Duration = 300
    backoff_rate: int | float = 2.0
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    retryable_errors: list[str | re.Pattern] | None = None
    retryable_error_types: list[type[Exception]] | None = None

    def __post_init__(self):
        self.initial_delay = duration_to_seconds(self.initial_delay, "initial_delay")
        self.max_delay = duration_to_seconds(self.max_delay, "max_delay")

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

            return RetryDecision.retry(final_delay)

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
            initial_delay=5,
            max_delay=60,
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
            initial_delay=5,
            max_delay=300,
            backoff_rate=2,
        ).build()

    @classmethod
    def critical(cls) -> Callable[[Exception, int], RetryDecision]:
        """Aggressive retries for critical operations."""
        return RetryStrategyBuilder(
            max_attempts=10,
            initial_delay=1,
            max_delay=60,
            backoff_rate=1.5,
            jitter_strategy=JitterStrategy.NONE,
        ).build()

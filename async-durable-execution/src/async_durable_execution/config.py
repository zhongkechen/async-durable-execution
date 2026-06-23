"""Configuration types."""

from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING

from .exceptions import ValidationError
from .models import RetryDecision

if TYPE_CHECKING:
    from collections.abc import Callable


def duration_to_seconds(duration: timedelta, field_name: str = "duration") -> int:
    """Convert a timedelta to whole seconds after validating it is non-negative."""
    total_seconds = duration.total_seconds()
    if total_seconds < 0:
        msg = f"{field_name} must be non-negative"
        raise ValidationError(msg)
    return int(total_seconds)


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
    initial_delay: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    max_delay: timedelta = field(
        default_factory=lambda: timedelta(minutes=5)
    )  # 5 minutes
    backoff_rate: int | float = 2.0
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

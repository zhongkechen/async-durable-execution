"""Core configuration types."""

from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import TypeAlias

from .exceptions import ValidationError

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

    def finalize_delay(self, base_delay: float) -> int:
        """Apply jitter, round up, and clamp to a minimum of 1 second."""
        return max(1, math.ceil(self.apply_jitter(base_delay)))


@dataclass
class _DelayStrategy:
    """Common delay configuration for retry and polling strategies."""

    max_attempts: int = 6
    initial_delay: Duration = 5
    max_delay: Duration = 60
    backoff_rate: int | float = 2
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    increment: Duration | None = None

    def __post_init__(self):
        self.initial_delay = duration_to_seconds(self.initial_delay, "initial_delay")
        self.max_delay = duration_to_seconds(self.max_delay, "max_delay")
        if self.increment is not None:
            self.increment = duration_to_seconds(self.increment, "increment")

    @property
    def initial_delay_seconds(self) -> int:
        """Get initial delay in seconds."""
        return duration_to_seconds(self.initial_delay, "initial_delay")

    @property
    def max_delay_seconds(self) -> int:
        """Get max delay in seconds."""
        return duration_to_seconds(self.max_delay, "max_delay")

    @property
    def increment_seconds(self) -> int | None:
        """Get linear delay increment in seconds."""
        if self.increment is None:
            return None
        return duration_to_seconds(self.increment, "increment")

    def calculate_delay(self, attempts_made: int) -> int:
        """Calculate a whole-second delay for exponential or linear strategies."""
        increment_seconds = self.increment_seconds
        if increment_seconds is None:
            base_delay: float = self.initial_delay_seconds * (
                self.backoff_rate ** (attempts_made - 1)
            )
        else:
            base_delay = self.initial_delay_seconds + increment_seconds * (
                attempts_made - 1
            )

        return self.jitter_strategy.finalize_delay(
            min(base_delay, self.max_delay_seconds)
        )


@dataclass
class RetryStrategy(_DelayStrategy):
    """Exponential-backoff retry strategy for durable operations."""

    retryable_errors: list[str | re.Pattern] | None = None
    retryable_error_types: list[type[Exception]] | None = None

    def __call__(self, error: Exception, attempts_made: int) -> Duration | None:
        """Return retry delay, or None if the error should not be retried."""
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

        if attempts_made >= self.max_attempts:
            return None

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
            return None

        return self.calculate_delay(attempts_made)

    @classmethod
    def none(cls) -> RetryStrategy:
        """No retries."""
        return cls(max_attempts=1, max_delay=300, backoff_rate=2.0)

    @classmethod
    def default(cls) -> RetryStrategy:
        """Default retries, used automatically when no retry strategy is provided."""
        return cls()

    @classmethod
    def transient(cls) -> RetryStrategy:
        """Quick retries for transient errors."""
        return cls(
            max_attempts=3,
            max_delay=300,
            backoff_rate=2,
            jitter_strategy=JitterStrategy.HALF,
        )

    @classmethod
    def resource_availability(cls) -> RetryStrategy:
        """Longer retries for resource availability."""
        return cls(
            max_attempts=5,
            initial_delay=5,
            max_delay=300,
            backoff_rate=2,
        )

    @classmethod
    def critical(cls) -> RetryStrategy:
        """Aggressive retries for critical operations."""
        return cls(
            max_attempts=10,
            initial_delay=1,
            max_delay=60,
            backoff_rate=1.5,
            jitter_strategy=JitterStrategy.NONE,
        )

    @classmethod
    def linear(cls) -> RetryStrategy:
        """Linearly increasing delay between retries: 1s, 2s, 3s, 4s, 5s."""
        return cls(
            max_attempts=6,
            initial_delay=1,
            increment=1,
            max_delay=300,
            backoff_rate=2,
            jitter_strategy=JitterStrategy.NONE,
        )

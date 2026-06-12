"""Ready-made wait strategies and wait creators."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Generic

from async_durable_execution.config import (
    JitterStrategy,
    T,
    duration_to_seconds,
)


if TYPE_CHECKING:
    from collections.abc import Callable

    from async_durable_execution.serdes import SerDes

Numeric = int | float


@dataclass
class WaitDecision:
    """Decision about whether to wait a step and with what delay."""

    should_wait: bool
    delay: timedelta

    def __post_init__(self):
        duration_to_seconds(self.delay)

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return duration_to_seconds(self.delay)

    @classmethod
    def wait(cls, delay: timedelta) -> WaitDecision:
        """Create a wait decision."""
        return cls(should_wait=True, delay=delay)

    @classmethod
    def no_wait(cls) -> WaitDecision:
        """Create a no-wait decision."""
        return cls(should_wait=False, delay=timedelta())


@dataclass
class WaitStrategyConfig(Generic[T]):
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


def create_wait_strategy(
    config: WaitStrategyConfig[T],
) -> Callable[[T, int], WaitDecision]:
    def wait_strategy(result: T, attempts_made: int) -> WaitDecision:
        # Check if condition is met
        if not config.should_continue_polling(result):
            return WaitDecision.no_wait()

        # Check if we've exceeded max attempts
        if attempts_made >= config.max_attempts:
            return WaitDecision.no_wait()

        # Calculate delay with exponential backoff
        base_delay: float = min(
            config.initial_delay_seconds * (config.backoff_rate ** (attempts_made - 1)),
            config.max_delay_seconds,
        )

        # Apply jitter to get final delay
        delay_with_jitter: float = config.jitter_strategy.apply_jitter(base_delay)

        # Round up and ensure minimum of 1 second
        final_delay: int = max(1, math.ceil(delay_with_jitter))

        return WaitDecision.wait(timedelta(seconds=final_delay))

    return wait_strategy


@dataclass(frozen=True)
class WaitForConditionDecision:
    """Decision about whether to continue waiting."""

    should_continue: bool
    delay: timedelta

    def __post_init__(self):
        duration_to_seconds(self.delay)

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return duration_to_seconds(self.delay)

    @classmethod
    def continue_waiting(cls, delay: timedelta) -> WaitForConditionDecision:
        """Create a decision to continue waiting for delay_seconds."""
        return cls(should_continue=True, delay=delay)

    @classmethod
    def stop_polling(cls) -> WaitForConditionDecision:
        """Create a decision to stop polling."""
        return cls(should_continue=False, delay=timedelta())


@dataclass(frozen=True)
class WaitForConditionConfig(Generic[T]):
    """Configuration for wait_for_condition."""

    wait_strategy: Callable[[T, int], WaitForConditionDecision]
    initial_state: T
    serdes: SerDes | None = None

"""1-12: Retry exhaustion (max attempts) - always fails, 4 total attempts."""

from datetime import timedelta
from async_durable_execution import (
    JitterStrategy,
    RetryStrategy,
    durable_callable,
    durable_execution,
    step,
)
from typing import Any


@durable_callable
async def always_fail() -> str:
    msg = "Always fails"
    raise RuntimeError(msg)


@durable_execution
async def handler(_event: Any) -> str:
    retry_strategy = RetryStrategy(
        max_attempts=4,
        initial_delay=timedelta(seconds=1),
        backoff_rate=1,
        jitter_strategy=JitterStrategy.NONE,
    )

    result: str = await step(always_fail(), retry_strategy=retry_strategy)
    return result

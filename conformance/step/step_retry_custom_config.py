"""1-14: Retry with custom config."""

from datetime import timedelta
from async_durable_execution import (
    JitterStrategy,
    RetryStrategy,
    durable_callable,
    durable_execution,
    get_step_context,
    step,
)
from typing import Any


@durable_callable
async def flaky() -> str:
    attempt = get_step_context().attempt or 1
    if attempt < 3:
        msg = f"Attempt {attempt} failed"
        raise RuntimeError(msg)
    return "finally succeeded"


@durable_execution
async def handler(_event: Any) -> str:
    retry_strategy = RetryStrategy(
        max_attempts=5,
        initial_delay=timedelta(seconds=2),
        backoff_rate=3,
        jitter_strategy=JitterStrategy.NONE,
    )

    result: str = await step(flaky(), retry_strategy=retry_strategy)
    return result

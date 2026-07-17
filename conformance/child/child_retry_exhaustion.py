"""3-8: Child context with step retry exhaustion (child fails)."""

from datetime import timedelta
from async_durable_execution import (
    JitterStrategy,
    RetryStrategy,
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
)
from typing import Any


@durable_callable
async def always_fail() -> str:
    msg = "Always fails"
    raise RuntimeError(msg)


@durable_callable
async def exhaust_child() -> str:
    retry_strategy = RetryStrategy(
        max_attempts=2,
        initial_delay=timedelta(seconds=1),
        backoff_rate=1,
        jitter_strategy=JitterStrategy.NONE,
    )

    return await step(always_fail(), retry_strategy=retry_strategy)


@durable_execution
async def handler(_event: Any) -> str:
    result: str = await run_in_child_context(exhaust_child(), name="exhaust-child")
    return result

"""3-7: Child context with step retry (fails then succeeds)."""

from async_durable_execution import (
    RetryStrategy,
    durable_callable,
    durable_execution,
    get_step_context,
    run_in_child_context,
    step,
)
from typing import Any


@durable_callable
async def unreliable_step(*, value: str) -> str:
    attempt = get_step_context().attempt or 1
    if attempt < 2:
        msg = f"Attempt {attempt} failed"
        raise RuntimeError(msg)
    return value


@durable_callable
async def retry_child(*, value: str) -> str:
    retry_strategy = RetryStrategy(max_attempts=3, retryable_error_types=[RuntimeError])

    return await step(
        unreliable_step(value=value),
        retry_strategy=retry_strategy,
    )


@durable_execution
async def handler(event: Any) -> str:
    result: str = await run_in_child_context(
        retry_child(value=str(event)), name="retry-child"
    )
    return result

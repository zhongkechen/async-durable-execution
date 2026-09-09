"""1-11: Step with retry."""

from async_durable_execution import (
    RetryStrategy,
    durable_callable,
    durable_execution,
    get_step_context,
    step,
)
from typing import Any


@durable_callable
async def unreliable_operation() -> str:
    attempt = get_step_context().attempt or 1
    if attempt < 2:
        msg = f"Attempt {attempt} failed"
        raise RuntimeError(msg)
    return "Operation succeeded"


@durable_execution
async def handler(_event: Any) -> str:
    retry_strategy = RetryStrategy(max_attempts=3, retryable_error_types=[RuntimeError])

    result: str = await step(unreliable_operation(), retry_strategy=retry_strategy)

    return result

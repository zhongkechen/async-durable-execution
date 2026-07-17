"""1-15: Retry specific exception."""

from datetime import timedelta
from async_durable_execution import (
    RetryStrategy,
    durable_callable,
    durable_execution,
    get_step_context,
    step,
)
from typing import Any


class TransientError(Exception):
    """Custom transient error that should be retried."""


@durable_callable
async def transient_on_first() -> str:
    if (get_step_context().attempt or 1) < 2:
        raise TransientError("Temporary failure")
    return "recovered from transient"


@durable_execution
async def handler(_event: Any) -> str:
    retry_strategy = RetryStrategy(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        retryable_error_types=[TransientError],
    )

    result: str = await step(transient_on_first(), retry_strategy=retry_strategy)
    return result

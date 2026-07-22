"""1-16: Retry specific exception (non-retryable fails) - TransientError not in retryable list."""

from datetime import timedelta
from async_durable_execution import (
    RetryStrategy,
    durable_callable,
    durable_execution,
    step,
)
from typing import Any


class TransientError(Exception):
    """Custom error that is NOT in the retryable list."""


class ValidationError(Exception):
    """The only error type configured as retryable."""


@durable_callable
async def throw_transient() -> str:
    raise TransientError("transient failure")


@durable_execution
async def handler(_event: Any) -> str:
    retry_strategy = RetryStrategy(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        retryable_error_types=[ValidationError],
    )

    result: str = await step(throw_transient(), retry_strategy=retry_strategy)
    return result

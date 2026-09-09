"""1-20: Error caught and handled (try/catch) - step fails, error caught, execution continues."""

from async_durable_execution import (
    RetryStrategy,
    durable_callable,
    durable_execution,
    step,
)
from typing import Any


@durable_callable
async def failing_step() -> str:
    msg = "Something went wrong"
    raise RuntimeError(msg)


@durable_callable
async def fallback_step() -> str:
    return "fallback_result"


@durable_execution
async def handler(_event: Any) -> str:
    try:
        await step(failing_step(), retry_strategy=RetryStrategy.none())
    except Exception:
        pass

    result: str = await step(fallback_step())
    return result

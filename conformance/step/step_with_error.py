"""1-19: Step with error (fails permanently)."""

from async_durable_execution import (
    RetryPresets,
    durable_callable,
    durable_execution,
    step,
)
from typing import Any


@durable_callable
async def failing_step() -> str:
    msg = "Something went wrong"
    raise RuntimeError(msg)


@durable_execution
async def handler(_event: Any) -> str:
    result: str = await step(failing_step(), retry_strategy=RetryPresets.none())
    return result

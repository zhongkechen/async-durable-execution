"""3-9: Child context replay (returns cached result)."""

from datetime import timedelta
from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
    wait,
)
from typing import Any


@durable_callable
async def compute_step(value: str) -> str:
    return value


@durable_callable
async def cached_child(value: str) -> str:
    return await step(compute_step(value))


@durable_execution
async def handler(event: Any) -> str:
    result: str = await run_in_child_context(cached_child(str(event)))
    await wait(timedelta(seconds=2))
    return result

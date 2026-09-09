"""3-13: Child context with wait inside - verify replay."""

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
async def wait_child(value: str) -> str:
    await wait(timedelta(seconds=1))
    return value


@durable_callable
async def after_child_step(value: str) -> str:
    return value


@durable_execution
async def handler(event: Any) -> str:
    await run_in_child_context(wait_child(str(event)), name="wait-child")
    result: str = await step(after_child_step(str(event)))
    return result

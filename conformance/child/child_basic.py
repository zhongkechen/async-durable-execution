"""3-1: Child context basic (single step inside)."""

from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
)
from typing import Any


@durable_callable
async def child_step(value: str) -> str:
    return value


@durable_callable
async def child_operation(value: str) -> str:
    return await step(child_step(value))


@durable_execution
async def handler(event: Any) -> str:
    result: str = await run_in_child_context(child_operation(str(event)))
    return result

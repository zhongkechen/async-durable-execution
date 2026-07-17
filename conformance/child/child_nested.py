"""3-6: Nested child contexts."""

from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
)
from typing import Any


@durable_callable
async def outer_step(value: str) -> str:
    return value


@durable_callable
async def inner_step(value: str) -> str:
    return value


@durable_callable
async def inner_child(value: str) -> str:
    return await step(inner_step(value))


@durable_callable
async def outer_child(value: str) -> str:
    await step(outer_step(value))
    inner_result: str = await run_in_child_context(inner_child(value), name="inner")
    return inner_result


@durable_execution
async def handler(event: Any) -> str:
    result: str = await run_in_child_context(outer_child(str(event)), name="outer")
    return result

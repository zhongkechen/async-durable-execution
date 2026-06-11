import asyncio
from typing import Any

from async_durable_execution.context import (
    DurableContext,
    durable_with_child_context,
)
from async_durable_execution.execution import durable_execution


async def multiply_by_two(value: int) -> int:
    return value * 2


@durable_with_child_context
async def child_operation(ctx: DurableContext, value: int) -> int:
    await asyncio.sleep(0)

    async def multiply(_) -> int:
        return await multiply_by_two(value)

    return ctx.step(multiply, name="multiply")


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    result = context.run_in_child_context(child_operation(5))
    return f"Child context result: {result}"

import asyncio
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    durable_execution,
    run_in_child_context,
    durable_child_context,
)


async def multiply_by_two(value: int) -> int:
    return value * 2


@durable_child_context
async def child_operation(value: int) -> int:
    await asyncio.sleep(0)

    @durable_step
    async def multiply() -> int:
        return await multiply_by_two(value)

    return await step(multiply(), name="multiply")


@durable_execution
async def handler(_event: Any) -> str:
    result = await run_in_child_context(
        child_operation(5),
        name="child_operation",
    )
    return f"Child context result: {result}"

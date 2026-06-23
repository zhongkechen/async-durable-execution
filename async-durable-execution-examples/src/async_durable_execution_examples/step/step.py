import asyncio
from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_callable,
    step,
    durable_execution,
)


@durable_callable
async def add_numbers(a: int, b: int) -> int:
    await asyncio.sleep(0)
    return a + b


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> int:
    result: int = await step(add_numbers(5, 3))
    return result

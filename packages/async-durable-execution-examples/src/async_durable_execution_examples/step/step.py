import asyncio
from functools import partial
from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


async def add_numbers(a: int, b: int) -> int:
    await asyncio.sleep(0)
    return a + b


@durable_execution
async def handler(_event: Any, context: DurableContext) -> int:
    result: int = await context.step(partial(add_numbers, 5, 3))
    return result

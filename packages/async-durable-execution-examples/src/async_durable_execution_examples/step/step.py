import asyncio
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    durable_execution,
)


@durable_step
async def add_numbers(a: int, b: int) -> int:
    await asyncio.sleep(0)
    return a + b


@durable_execution
async def handler(_event: Any) -> int:
    result: int = await step(add_numbers(5, 3))
    return result

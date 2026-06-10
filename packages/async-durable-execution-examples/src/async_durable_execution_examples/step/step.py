import asyncio
from typing import Any

from async_durable_execution.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from async_durable_execution.execution import durable_execution


@durable_step
async def add_numbers(_step_context: StepContext, a: int, b: int) -> int:
    await asyncio.sleep(0)
    return a + b


@durable_execution
async def handler(_event: Any, context: DurableContext) -> int:
    result: int = context.step(add_numbers(5, 3))
    return result

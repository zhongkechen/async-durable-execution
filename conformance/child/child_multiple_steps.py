"""3-3: Child context with multiple sequential steps."""

from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
)
from typing import Any


@durable_callable
async def first_step(value: str) -> str:
    return value


@durable_callable
async def second_step(value: str) -> str:
    return value


@durable_callable
async def multi_step_child(value: str) -> str:
    result1: str = await step(first_step(value))
    result2: str = await step(second_step(result1))
    return result2


@durable_execution
async def handler(event: Any) -> str:
    result: str = await run_in_child_context(
        multi_step_child(str(event)), name="multi-step"
    )
    return result

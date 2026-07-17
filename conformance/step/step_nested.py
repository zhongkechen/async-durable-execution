"""1-3: Sequential steps where the second depends on the first."""

from async_durable_execution import durable_callable, durable_execution, step
from typing import Any


@durable_callable
async def step_one() -> str:
    return "first"


@durable_callable
async def step_two(previous: str) -> str:
    return f"{previous}_second"


@durable_execution
async def handler(_event: Any) -> str:
    result1: str = await step(step_one())
    result2: str = await step(step_two(result1))
    return result2

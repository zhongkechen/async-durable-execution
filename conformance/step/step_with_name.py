"""1-2: Step with explicit name parameter."""

from async_durable_execution import durable_callable, durable_execution, step
from typing import Any


@durable_callable
async def greet(name: str) -> str:
    return f"Hello, {name}!"


@durable_execution
async def handler(event: Any) -> str:
    result: str = await step(greet(event), name="custom_step_name")
    return result

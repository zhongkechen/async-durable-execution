"""1-1: Step basic (succeeds on first attempt)."""

from async_durable_execution import durable_callable, durable_execution, step
from typing import Any


@durable_callable
async def greet(name: str) -> str:
    return f"Hello, {name}!"


@durable_execution
async def handler(event: Any) -> str:
    result: str = await step(greet(event))
    return result

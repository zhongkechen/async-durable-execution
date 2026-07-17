"""1-5: Undefined/null result."""

from async_durable_execution import durable_callable, durable_execution, step
from typing import Any


@durable_callable
async def do_nothing() -> None:
    return None


@durable_execution
async def handler(_event: Any) -> None:
    result = await step(do_nothing())
    return result

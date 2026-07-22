"""1-8: Step and wait with replay."""

from datetime import timedelta
from async_durable_execution import durable_callable, durable_execution, step, wait
from typing import Any


@durable_callable
async def compute() -> str:
    return "computed"


@durable_execution
async def handler(_event: Any) -> str:
    result: str = await step(compute())
    await wait(timedelta(seconds=2))
    return result

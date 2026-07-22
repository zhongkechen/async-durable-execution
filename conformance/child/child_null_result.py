"""3-16: Child context returning null."""

from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
)
from typing import Any


@durable_callable
async def null_child() -> None:
    return None


@durable_execution
async def handler(_event: Any) -> None:
    result = await run_in_child_context(null_child(), name="null-child")
    return result

"""3-15: Child context error without step (error thrown directly in child body)."""

from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
)
from typing import Any


@durable_callable
async def direct_error() -> str:
    msg = "direct error"
    raise RuntimeError(msg)


@durable_execution
async def handler(_event: Any) -> str:
    result: str = await run_in_child_context(direct_error(), name="direct-error")
    return result

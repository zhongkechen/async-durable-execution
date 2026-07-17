"""3-4: Child context error (step fails, execution fails)."""

from async_durable_execution import (
    RetryStrategy,
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
)
from typing import Any


@durable_callable
async def failing_step() -> str:
    msg = "Child step failed"
    raise RuntimeError(msg)


@durable_callable
async def failing_child() -> str:
    return await step(failing_step(), retry_strategy=RetryStrategy.none())


@durable_execution
async def handler(_event: Any) -> str:
    result: str = await run_in_child_context(failing_child(), name="failing-child")
    return result

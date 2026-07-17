"""3-10: Child context with step and wait inside."""

from datetime import timedelta
from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    step,
    wait,
)
from typing import Any


@durable_callable
async def compute_step(value: str) -> str:
    return value


@durable_callable
async def mixed_ops_child(value: str) -> str:
    await step(compute_step(value))
    await wait(timedelta(seconds=2))
    return value


@durable_execution
async def handler(event: Any) -> str:
    result: str = await run_in_child_context(
        mixed_ops_child(str(event)), name="mixed-ops"
    )
    return result

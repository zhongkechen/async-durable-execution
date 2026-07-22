"""3-5: Child context error caught (try/catch, execution succeeds)."""

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


@durable_callable
async def recovery_step(value: str) -> str:
    return value


@durable_execution
async def handler(event: Any) -> str:
    try:
        await run_in_child_context(failing_child())
    except Exception:
        pass

    result: str = await step(recovery_step(str(event)))
    return result

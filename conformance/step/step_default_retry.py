"""1-13: Default retry strategy."""

from async_durable_execution import (
    durable_callable,
    durable_execution,
    get_step_context,
    step,
)
from typing import Any


@durable_callable
async def unreliable() -> str:
    attempt = get_step_context().attempt or 1
    if attempt < 3:
        msg = f"Attempt {attempt} failed"
        raise RuntimeError(msg)
    return "recovered"


@durable_execution
async def handler(_event: Any) -> str:
    # Step with no explicit retry config — uses SDK default
    result: str = await step(unreliable())
    return result

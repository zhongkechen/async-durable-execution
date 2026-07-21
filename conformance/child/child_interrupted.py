"""3-12: Child context interrupted and re-executed."""

import asyncio
from async_durable_execution import (
    RetryStrategy,
    durable_callable,
    durable_execution,
    get_current_context,
    run_in_child_context,
    step,
)
import os
from typing import Any


@durable_callable
async def crashable_step(*, should_crash: bool, value: str) -> str:
    if should_crash:
        # Sleep to allow checkpoint to be sent before crash
        await asyncio.sleep(1)
        # Simulate Lambda crash
        os._exit(1)
    return value


@durable_callable
async def interrupted_child(*, value: str) -> str:
    # Capture child replay state before step() binds its own operation context.
    should_crash = not get_current_context().is_replaying()
    return await step(
        crashable_step(should_crash=should_crash, value=value),
        retry_strategy=RetryStrategy.none(),
    )


@durable_execution
async def handler(event: Any) -> str:
    result: str = await run_in_child_context(interrupted_child(value=str(event)))
    return result

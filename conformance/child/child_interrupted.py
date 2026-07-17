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

from support.attempts import increment_attempt


@durable_callable
async def crashable_step(*, execution_id: str, value: str) -> str:
    attempt_count = await increment_attempt(execution_id)

    if attempt_count < 2:
        # Sleep to allow checkpoint to be sent before crash
        await asyncio.sleep(1)
        # Simulate Lambda crash
        os._exit(1)
    return value


@durable_callable
async def interrupted_child(*, execution_id: str, value: str) -> str:
    return await step(
        crashable_step(execution_id=execution_id, value=value),
        retry_strategy=RetryStrategy.none(),
    )


@durable_execution
async def handler(event: Any) -> str:
    execution_id = get_current_context().durable_execution_arn

    result: str = await run_in_child_context(
        interrupted_child(execution_id=execution_id, value=str(event))
    )
    return result

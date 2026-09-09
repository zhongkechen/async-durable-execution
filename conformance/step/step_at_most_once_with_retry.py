"""1-18: AtMostOnce interrupted with retry."""

import asyncio
from datetime import timedelta
from async_durable_execution import (
    RetryStrategy,
    StepSemantics,
    durable_callable,
    durable_execution,
    get_step_context,
    step,
)
import os
from typing import Any


@durable_callable
async def at_most_once_step(*, input_1: str) -> str:
    attempt = get_step_context().attempt or 1
    # Print input to stdout each time step executes
    print(input_1, flush=True)
    await asyncio.sleep(1)  # Allow time for logs to flush to CloudWatch

    if attempt < 2:
        # First attempt: simulate Lambda crash
        os._exit(1)
    # Second attempt (retry): succeed
    return "succeeded on second attempt"


@durable_execution
async def handler(event: Any) -> str:
    retry_strategy = RetryStrategy(max_attempts=3, initial_delay=timedelta(seconds=1))

    result: str = await step(
        at_most_once_step(input_1=str(event)),
        retry_strategy=retry_strategy,
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )
    return result

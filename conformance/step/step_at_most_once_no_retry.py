"""1-17: AtMostOnce interrupted (no retry) - Lambda crash, StepInterruptedError, fails permanently."""

import asyncio
from async_durable_execution import (
    RetryStrategy,
    StepSemantics,
    durable_callable,
    durable_execution,
    step,
)
import os
from typing import Any


@durable_callable
async def at_most_once_flaky_step(*, input_1: str) -> str:
    print(input_1, flush=True)
    await asyncio.sleep(1)  # Allow time for logs to flush to CloudWatch
    os._exit(1)  # Simulate Lambda crash
    return "unreachable"


@durable_execution
async def handler(event: Any) -> str:
    result: str = await step(
        at_most_once_flaky_step(input_1=str(event)),
        name="at_most_once_flaky_step",
        retry_strategy=RetryStrategy.none(),
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )
    return result

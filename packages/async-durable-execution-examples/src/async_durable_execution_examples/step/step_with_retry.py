from typing import Any

from async_durable_execution.config import StepConfig
from async_durable_execution.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from async_durable_execution.execution import durable_execution
from async_durable_execution.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_step
async def unreliable_operation(
    step_context: StepContext,
) -> str:
    # Retry behavior is derived from the current step attempt so it remains
    # deterministic for each durable execution and safe across warm Lambdas.
    attempt = step_context.attempt or 1
    if attempt < 2:
        msg = f"Attempt {attempt} failed"
        raise RuntimeError(msg)
    return "Operation succeeded"


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        retryable_error_types=[RuntimeError],
    )

    result: str = await context.step(
        unreliable_operation(),
        config=StepConfig(create_retry_strategy(retry_config)),
    )

    return result

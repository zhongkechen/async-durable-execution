from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    StepConfig,
    durable_execution,
    RetryStrategyConfig,
    create_retry_strategy,
    get_attempt,
)


@durable_step
async def unreliable_operation() -> str:
    # Retry behavior is derived from the current step attempt so it remains
    # deterministic for each durable execution and safe across warm Lambdas.
    attempt = get_attempt() or 1
    if attempt < 2:
        msg = f"Attempt {attempt} failed"
        raise RuntimeError(msg)
    return "Operation succeeded"


@durable_execution
async def handler(_event: Any) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        retryable_error_types=[RuntimeError],
    )

    result: str = await step(
        unreliable_operation(),
        config=StepConfig(create_retry_strategy(retry_config)),
    )

    return result

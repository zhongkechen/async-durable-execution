from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    StepConfig,
    durable_execution,
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_execution
async def handler(_event: Any) -> str:
    # Step with exponential backoff retry strategy
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        max_delay=timedelta(seconds=10),
        backoff_rate=2.0,
    )

    step_config = StepConfig(retry_strategy=create_retry_strategy(retry_config))

    @durable_step
    async def retry_step() -> str:
        return "Step with exponential backoff"

    result = await step(retry_step(), name="retry_step", config=step_config)
    return f"Result: {result}"

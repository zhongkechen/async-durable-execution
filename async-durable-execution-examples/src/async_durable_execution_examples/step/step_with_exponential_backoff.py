from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    step,
    durable_execution,
    RetryStrategyBuilder,
)


@durable_execution
async def handler(_event: Any) -> str:
    # Step with exponential backoff retry strategy
    retry_config = RetryStrategyBuilder(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        max_delay=timedelta(seconds=10),
        backoff_rate=2.0,
    )

    @durable_callable
    async def retry_step() -> str:
        return "Step with exponential backoff"

    result = await step(
        retry_step(),
        name="retry_step",
        retry_strategy=retry_config.build(),
    )
    return f"Result: {result}"

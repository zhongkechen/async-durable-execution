from datetime import timedelta
from typing import Any

from async_durable_execution.config import StepConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution
from async_durable_execution.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    # Step with exponential backoff retry strategy
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        max_delay=timedelta(seconds=10),
        backoff_rate=2.0,
    )

    step_config = StepConfig(retry_strategy=create_retry_strategy(retry_config))

    async def retry_step(_) -> str:
        return "Step with exponential backoff"

    result = await context.step(retry_step, name="retry_step", config=step_config)
    return f"Result: {result}"

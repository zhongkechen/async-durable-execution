"""Demonstrates runInChildContext with a failing step followed by a successful wait."""

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
async def handler(_event: Any, context: DurableContext) -> dict[str, bool]:
    """Handler demonstrating runInChildContext with failing step."""

    async def child_with_failure(ctx: DurableContext) -> None:
        """Child context with a failing step."""

        retry_config = RetryStrategyConfig(
            max_attempts=3,
            initial_delay=timedelta(seconds=1),
            max_delay=timedelta(seconds=10),
            backoff_rate=2.0,
        )
        step_config = StepConfig(retry_strategy=create_retry_strategy(retry_config))

        async def failing_step(_) -> None:
            """Step that always fails."""
            raise Exception("Step failed in child context")

        await ctx.step(
            failing_step,
            name="failing-step",
            config=step_config,
        )

    try:
        await context.run_in_child_context(
            child_with_failure,
            name="child-with-failure",
        )
    except Exception as error:
        # Catch and ignore child context and step errors
        result = {"success": True, "error": str(error)}

    await context.wait(timedelta(seconds=1), name="wait-after-failure")

    return result

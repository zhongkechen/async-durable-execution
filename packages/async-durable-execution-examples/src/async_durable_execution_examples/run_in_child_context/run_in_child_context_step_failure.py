"""Demonstrates runInChildContext with a failing step followed by a successful wait."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    StepConfig,
    durable_execution,
    RetryStrategyConfig,
    create_retry_strategy,
    run_in_child_context,
    wait,
)


@durable_execution
async def handler(_event: Any) -> dict[str, bool]:
    """Handler demonstrating runInChildContext with failing step."""

    async def child_with_failure() -> None:
        """Child context with a failing step."""

        retry_config = RetryStrategyConfig(
            max_attempts=3,
            initial_delay=timedelta(seconds=1),
            max_delay=timedelta(seconds=10),
            backoff_rate=2.0,
        )
        step_config = StepConfig(retry_strategy=create_retry_strategy(retry_config))

        @durable_step
        async def failing_step() -> None:
            """Step that always fails."""
            raise Exception("Step failed in child context")

        await step(
            failing_step(),
            name="failing-step",
            config=step_config,
        )

    try:
        await run_in_child_context(
            child_with_failure,
            name="child-with-failure",
        )
    except Exception as error:
        # Catch and ignore child context and step errors
        result = {"success": True, "error": str(error)}

    await wait(timedelta(seconds=1), name="wait-after-failure")

    return result

"""Demonstrates runInChildContext with a failing step followed by a successful wait."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    StepConfig,
    durable_execution,
    RetryStrategyBuilder,
    run_in_child_context,
    wait,
    durable_child_context,
)


@durable_step
async def failing_step() -> None:
    """Step that always fails."""
    raise Exception("Step failed in child context")


@durable_child_context
async def child_with_failure() -> None:
    """Child context with a failing step."""

    retry_config = RetryStrategyBuilder(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        max_delay=timedelta(seconds=10),
        backoff_rate=2.0,
    )
    step_config = StepConfig(retry_strategy=retry_config.build())

    await step(
        failing_step(),
        name="failing-step",
        config=step_config,
    )


@durable_execution
async def handler(_event: Any) -> dict[str, bool]:
    """Handler demonstrating runInChildContext with failing step."""
    try:
        await run_in_child_context(
            child_with_failure(),
            name="child-with-failure",
        )
    except Exception as error:
        # Catch and ignore child context and step errors
        result = {"success": True, "error": str(error)}

    await wait(timedelta(seconds=1), name="wait-after-failure")

    return result

"""Example demonstrating multiple steps with retry logic."""

from datetime import timedelta
from itertools import count
from typing import Any

from async_durable_execution.config import StepConfig
from async_durable_execution.context import DurableContext, StepContext
from async_durable_execution.execution import durable_execution
from async_durable_execution.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


# Counter for deterministic behavior across retries
_attempts = count(1)  # starts from 1


async def simulated_get_item(
    _step_context: StepContext, name: str
) -> dict[str, Any] | None:
    """Simulate getting an item with deterministic counter-based behavior."""
    # Use counter for deterministic behavior
    attempt = next(_attempts)

    # Fail on first attempt
    if attempt == 1:
        msg = "Random failure"
        raise RuntimeError(msg)

    # Return None on second attempt (poll 1)
    if attempt == 2:
        return None

    # Return item on third attempt (poll 2, after retry)
    return {"id": name, "data": "item data"}


@durable_execution
async def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating polling with retry logic."""
    name = event.get("name", "test-item")

    # Retry configuration for steps
    retry_config = RetryStrategyConfig(
        max_attempts=5,
        retryable_error_types=[RuntimeError],
    )

    step_config = StepConfig(create_retry_strategy(retry_config))

    item = None
    poll_count = 0
    max_polls = 5

    try:
        while poll_count < max_polls:
            poll_count += 1

            async def get_item(step_context: StepContext, item_name: str = name):
                return await simulated_get_item(step_context, item_name)

            # Try to get the item with retry
            get_response = await context.step(
                get_item,
                name=f"get_item_poll_{poll_count}",
                config=step_config,
            )

            # Did we find the item?
            if get_response:
                item = get_response
                break

            # Wait 1 second until next poll
            await context.wait(timedelta(seconds=1))

    except RuntimeError as e:
        # Retries exhausted
        return {"error": "DDB Retries Exhausted", "message": str(e)}

    if not item:
        return {"error": "Item Not Found"}

    # We found the item!
    return {"success": True, "item": item, "pollsRequired": poll_count}

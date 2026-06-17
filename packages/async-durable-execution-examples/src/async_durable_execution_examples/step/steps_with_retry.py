"""Example demonstrating multiple steps with retry logic."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    wait,
    StepConfig,
    durable_execution,
    RetryStrategyBuilder,
    get_attempt,
)


async def simulated_get_item(name: str, poll_count: int) -> dict[str, Any] | None:
    """Simulate getting an item with deterministic per-poll retry behavior."""
    attempt = get_attempt() or 1

    # Poll 1 fails once, then returns None on retry so the workflow polls again.
    if poll_count == 1 and attempt == 1:
        msg = "Random failure"
        raise RuntimeError(msg)

    if poll_count == 1:
        return None

    # Poll 2 succeeds immediately.
    return {"id": name, "data": "item data"}


@durable_execution
async def handler(event: Any) -> dict[str, Any]:
    """Handler demonstrating polling with retry logic."""
    name = event.get("name", "test-item")

    # Retry configuration for steps
    retry_config = RetryStrategyBuilder(
        max_attempts=5,
        retryable_error_types=[RuntimeError],
    )

    step_config = StepConfig(retry_config.build())

    item = None
    poll_count = 0
    max_polls = 5

    try:
        while poll_count < max_polls:
            poll_count += 1

            @durable_step
            async def get_item(item_name: str = name):
                return await simulated_get_item(item_name, poll_count)

            # Try to get the item with retry
            get_response = await step(
                get_item(),
                name=f"get_item_poll_{poll_count}",
                config=step_config,
            )

            # Did we find the item?
            if get_response:
                item = get_response
                break

            # Wait 1 second until next poll
            await wait(timedelta(seconds=1))

    except RuntimeError as e:
        # Retries exhausted
        return {"error": "DDB Retries Exhausted", "message": str(e)}

    if not item:
        return {"error": "Item Not Found"}

    # We found the item!
    return {"success": True, "item": item, "pollsRequired": poll_count}

"""Example demonstrating map with failure tolerance."""

import asyncio
from typing import Any

from async_durable_execution import (
    CompletionConfig,
    RetryStrategy,
    durable_execution,
    durable_callable,
    get_current_context,
    map,
    step,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Process items with failure tolerance."""
    items = list(range(1, 11))  # [1, 2, 3, ..., 10]

    # Disable retries so failures happen immediately
    retry_strategy = RetryStrategy(max_attempts=1)

    async def process_item(item: int) -> int:
        await asyncio.sleep(0)
        map_context = get_current_context()

        @durable_callable
        async def run() -> int:
            return await _process_with_failures(item)

        return await step(
            run(),
            name=f"item_{map_context.index}",
            retry_strategy=retry_strategy,
        )

    results = await map(
        func=process_item,
        items=items,
        name="map_with_tolerance",
        max_concurrency=5,
        completion_config=CompletionConfig(tolerated_failure_count=3),
    )

    return {
        "success_count": results.success_count,
        "failure_count": results.failure_count,
        "succeeded": [item.result for item in results.succeeded()],
        "failed_count": len(results.failed()),
        "completion_reason": results.completion_reason.value,
    }


async def _process_with_failures(item: int) -> int:
    """Process item - fails for items 3, 6, 9."""
    if item % 3 == 0:
        raise ValueError(f"Item {item} failed")
    return item * 2

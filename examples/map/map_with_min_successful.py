"""Example demonstrating map with min_successful completion config."""

import asyncio
from typing import Any

from async_durable_execution import (
    durable_callable,
    get_map_item_context,
    step,
    CompletionConfig,
    durable_execution,
    map,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Process items with min_successful threshold."""
    items = list(range(1, 11))  # [1, 2, 3, ..., 10]

    async def process_item(item: int) -> int:
        await asyncio.sleep(0)
        map_context = get_map_item_context()

        @durable_callable
        async def run() -> int:
            return await _process_item(item)

        return await step(run(), name=f"item_{map_context.index}")

    results = await map(
        func=process_item,
        items=items,
        name="map_min_successful",
        max_concurrency=5,
        completion_config=CompletionConfig(min_successful=6),
    )

    return {
        "success_count": results.success_count,
        "failure_count": results.failure_count,
        "total_count": results.total_count,
        "results": results.get_results(),
        "completion_reason": results.completion_reason.value,
    }


async def _process_item(item: int) -> int:
    """Process item - fails for items 7, 8, 9."""
    if item in [7, 8, 9]:
        raise ValueError(f"Item {item} failed")
    return item * 2

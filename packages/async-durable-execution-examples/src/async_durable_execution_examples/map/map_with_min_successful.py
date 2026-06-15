"""Example demonstrating map with min_successful completion config."""

import asyncio
from typing import Any

from async_durable_execution.config import CompletionConfig, MapConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Process items with min_successful threshold."""
    items = list(range(1, 11))  # [1, 2, 3, ..., 10]

    # Configure to complete when 6 items succeed
    config = MapConfig(
        max_concurrency=5,
        completion_config=CompletionConfig(min_successful=6),
    )

    async def process_item(ctx: DurableContext, item: int, index: int, _) -> int:
        await asyncio.sleep(0)

        async def run() -> int:
            return await _process_item(item)

        return await ctx.step(run, name=f"item_{index}")

    results = await context.map(
        inputs=items,
        func=process_item,
        name="map_min_successful",
        config=config,
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

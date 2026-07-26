"""Example demonstrating map with maxConcurrency limit."""

import asyncio
from typing import Any

from async_durable_execution import (
    durable_callable,
    get_current_context,
    step,
    durable_execution,
    map,
)


@durable_execution
async def handler(_event: Any) -> list[int]:
    """Process items with concurrency limit of 3."""
    items = list(range(1, 11))  # [1, 2, 3, ..., 10]

    async def process_item(item: int) -> int:
        await asyncio.sleep(0)
        map_context = get_current_context()

        @durable_callable
        async def triple() -> int:
            return item * 3

        return await step(triple(), name=f"process_{map_context.index}")

    # Extract results immediately to avoid BatchResult serialization
    return (
        await map(
            func=process_item,
            items=items,
            name="map_with_concurrency",
            max_concurrency=3,
        )
    ).get_results()

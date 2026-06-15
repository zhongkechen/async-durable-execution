"""Example demonstrating map with maxConcurrency limit."""

import asyncio
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    MapConfig,
    durable_execution,
    map,
)


@durable_execution
async def handler(_event: Any) -> list[int]:
    """Process items with concurrency limit of 3."""
    items = list(range(1, 11))  # [1, 2, 3, ..., 10]

    async def process_item(item: int, index: int, _) -> int:
        await asyncio.sleep(0)

        @durable_step
        async def triple() -> int:
            return item * 3

        return await step(triple(), name=f"process_{index}")

    # Extract results immediately to avoid BatchResult serialization
    return (
        await map(
            inputs=items,
            func=process_item,
            name="map_with_concurrency",
            config=MapConfig(max_concurrency=3),
        )
    ).get_results()

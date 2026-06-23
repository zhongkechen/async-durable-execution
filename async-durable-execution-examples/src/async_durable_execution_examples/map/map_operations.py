"""Example demonstrating map operations for processing collections durably."""

import asyncio
from typing import Any

from async_durable_execution import (
    durable_callable,
    get_current_context,
    step,
    MapConfig,
    durable_execution,
    map,
)


@durable_execution
async def handler(_event: Any) -> list[int]:
    """Process a list of items using map()."""
    items = [1, 2, 3, 4, 5]

    async def process_item(item: int) -> int:
        await asyncio.sleep(0)
        map_context = get_current_context()

        @durable_callable
        async def double() -> int:
            return item * 2

        return await step(double(), name=f"map_item_{map_context.index}")

    # Use map() to process items concurrently and extract results immediately
    return (
        await map(
            inputs=items,
            func=process_item,
            name="map_operation",
            config=MapConfig(max_concurrency=2),
        )
    ).get_results()

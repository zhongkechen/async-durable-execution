"""Example demonstrating map operations for processing collections durably."""

import asyncio
from typing import Any

from aws_durable_execution_sdk_python.config import MapConfig, NestingType
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> list[int]:
    """Process a list of items using context.map()."""
    items = [1, 2, 3, 4, 5]

    async def process_item(ctx: DurableContext, item: int, index: int, _) -> int:
        await asyncio.sleep(0)

        async def double(_) -> int:
            return item * 2

        return ctx.step(double, name=f"map_item_{index}")

    # Use context.map() to process items concurrently and extract results immediately
    return context.map(
        inputs=items,
        func=process_item,
        name="map_operation",
        config=MapConfig(max_concurrency=2, nesting_type=NestingType.FLAT),
    ).get_results()

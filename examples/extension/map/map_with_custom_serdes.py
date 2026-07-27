"""Example demonstrating map with custom serdes."""

import asyncio
import json
from typing import Any

from async_durable_execution import (
    durable_callable,
    get_map_item_context,
    step,
    durable_execution,
    SerDes,
    map,
)


class CustomItemSerDes(SerDes[dict[str, Any]]):
    """Custom serializer for individual items that adds metadata."""

    async def serialize(self, value: dict[str, Any]) -> str:
        # Add custom metadata during serialization
        wrapped = {"data": value, "serialized_by": "CustomItemSerDes", "version": "1.0"}

        return json.dumps(wrapped)

    async def deserialize(self, payload: str) -> dict[str, Any]:
        wrapped = json.loads(payload)
        # Extract the original data
        return wrapped["data"]


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Process items with custom item serialization.

    This example demonstrates using item_serdes to customize serialization
    of individual item results, while using default serialization for the
    overall BatchResult.
    """
    items = [
        {"id": 1, "name": "item1"},
        {"id": 2, "name": "item2"},
        {"id": 3, "name": "item3"},
    ]

    async def process_item(item: dict[str, Any]) -> dict[str, Any]:
        await asyncio.sleep(0)
        map_context = get_map_item_context()

        @durable_callable
        async def build_result() -> dict[str, Any]:
            return {
                "processed": item["name"],
                "index": map_context.index,
                "doubled_id": item["id"] * 2,
            }

        return await step(build_result(), name=f"process_{map_context.index}")

    results = await map(
        func=process_item,
        items=items,
        name="map_with_custom_serdes",
        item_serdes=CustomItemSerDes(),
    )

    return {
        "success_count": results.success_count,
        "results": results.get_results(),
        "processed_names": [r["processed"] for r in results.get_results()],
    }

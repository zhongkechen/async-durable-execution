"""Example demonstrating map with custom serdes."""

import asyncio
import json
from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_callable,
    step,
    MapConfig,
    durable_execution,
    SerDes,
    SerDesContext,
    map,
)


class CustomItemSerDes(SerDes[dict[str, Any]]):
    """Custom serializer for individual items that adds metadata."""

    def serialize(self, value: dict[str, Any], _: SerDesContext) -> str:
        # Add custom metadata during serialization
        wrapped = {"data": value, "serialized_by": "CustomItemSerDes", "version": "1.0"}

        return json.dumps(wrapped)

    def deserialize(self, payload: str, _: SerDesContext) -> dict[str, Any]:
        wrapped = json.loads(payload)
        # Extract the original data
        return wrapped["data"]


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> dict[str, Any]:
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

    # Use custom serdes for individual items only
    # The BatchResult will use default JSON serialization
    config = MapConfig(item_serdes=CustomItemSerDes())

    async def process_item(item: dict[str, Any], index: int, _) -> dict[str, Any]:
        await asyncio.sleep(0)

        @durable_callable
        async def build_result() -> dict[str, Any]:
            return {
                "processed": item["name"],
                "index": index,
                "doubled_id": item["id"] * 2,
            }

        return await step(build_result(), name=f"process_{index}")

    results = await map(
        inputs=items,
        func=process_item,
        name="map_with_custom_serdes",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "results": results.get_results(),
        "processed_names": [r["processed"] for r in results.get_results()],
    }

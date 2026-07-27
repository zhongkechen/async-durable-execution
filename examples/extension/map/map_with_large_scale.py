"""Test map with 50 iterations, each returning 100KB data."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    get_map_item_context,
    step,
    durable_execution,
    map,
    wait,
)


async def generate_large_string(size_in_kb: int) -> str:
    """Generate a string of approximately the specified size in KB."""
    return "A" * 1024 * size_in_kb


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating large scale map with substantial data."""
    # Create array of 50 items (more manageable for testing)
    items = list(range(1, 51))  # 1 to 50
    data = await generate_large_string(100)

    async def process_item(item: int) -> dict[str, Any]:
        map_context = get_map_item_context()

        @durable_callable
        async def build_result() -> dict[str, Any]:
            return {
                "itemId": item,
                "index": map_context.index,
                "dataSize": len(data),
                "data": data,
                "processed": True,
            }

        return await step(build_result())

    results = await map(
        func=process_item,
        items=items,
        name="large-scale-map",
        max_concurrency=10,
    )

    await wait(timedelta(seconds=1), name="wait1")

    # Process results immediately after map operation
    # Note: After wait operations, the BatchResult may be summarized
    final_results = results.get_results()
    total_data_size = sum(result["dataSize"] for result in final_results)
    all_items_processed = all(result["processed"] for result in final_results)

    total_size_in_mb = round(total_data_size / (1024 * 1024))

    summary = {
        "itemsProcessed": results.success_count,
        "totalDataSizeMB": total_size_in_mb,
        "totalDataSizeBytes": total_data_size,
        "maxConcurrency": 10,
        "averageItemSize": round(total_data_size / results.success_count),
        "allItemsProcessed": all_items_processed,
    }

    await wait(timedelta(seconds=1), name="wait2")

    return {
        "success": True,
        "message": "Successfully processed 50 items with substantial data using map",
        "summary": summary,
    }

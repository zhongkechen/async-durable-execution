"""Example demonstrating map with batch-level serdes."""

import asyncio
import json
from typing import Any

from async_durable_execution import (
    durable_callable,
    get_current_context,
    step,
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
    durable_execution,
    ErrorObject,
    JsonSerDes,
    SerDes,
    map,
)


class CustomBatchSerDes(SerDes[BatchResult]):
    """Custom serializer for the entire BatchResult."""

    async def serialize(self, value: BatchResult) -> str:
        # Serialize BatchResult with custom metadata

        wrapped = {
            "batch_metadata": {
                "serializer": "CustomBatchSerDes",
                "version": "2.0",
                "total_items": len(value.get_results()),
            },
            "success_count": value.success_count,
            "failure_count": value.failure_count,
            "results": value.get_results(),
            "errors": [e.to_dict() if e else None for e in value.get_errors()],
        }
        return json.dumps(wrapped)

    async def deserialize(self, payload: str) -> BatchResult:
        wrapped = json.loads(payload)
        batch_items = []
        results = wrapped["results"]
        errors = wrapped["errors"]

        for i, result in enumerate(results):
            error = errors[i] if i < len(errors) else None
            if error:
                batch_items.append(
                    BatchItem(
                        index=i,
                        status=BatchItemStatus.FAILED,
                        result=None,
                        error=ErrorObject.from_dict(error) if error else None,
                    )
                )
            else:
                batch_items.append(
                    BatchItem(
                        index=i,
                        status=BatchItemStatus.SUCCEEDED,
                        result=result,
                        error=None,
                    )
                )

        # Infer completion reason (assume ALL_COMPLETED if all succeeded)
        completion_reason = (
            CompletionReason.ALL_COMPLETED
            if wrapped["failure_count"] == 0
            else CompletionReason.FAILURE_TOLERANCE_EXCEEDED
        )

        return BatchResult(all=batch_items, completion_reason=completion_reason)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Process items with custom batch-level serialization."""
    items = [10, 20, 30, 40]

    async def process_item(item: int) -> int:
        await asyncio.sleep(0)
        map_context = get_current_context()

        @durable_callable
        async def double() -> int:
            return item * 2

        return await step(double(), name=f"double_{map_context.index}")

    results = await map(
        func=process_item,
        items=items,
        name="map_with_batch_serdes",
        serdes=CustomBatchSerDes(),
        item_serdes=JsonSerDes(),
    )

    return {
        "success_count": results.success_count,
        "results": results.get_results(),
        "sum": sum(results.get_results()),
    }

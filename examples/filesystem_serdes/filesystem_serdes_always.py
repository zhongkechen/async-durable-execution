"""Store durable step results on a shared filesystem across replay."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    FileSystemSerDesStage,
    FileSystemSerDesStageConfig,
    JsonSerDes,
    PreviewConfig,
    PreviewField,
    PreviewMode,
    durable_callable,
    durable_execution,
    step,
    wait,
)


@durable_callable
async def build_order(order_id: str) -> dict[str, Any]:
    """Build an order whose checkpoint is stored outside the execution history."""
    return {
        "order_id": order_id,
        "customer_email": "customer@example.com",
        "items": [
            {
                "sku": f"item-{index:02d}",
                "quantity": index + 1,
                "description": "filesystem-serdes-payload-" * 64,
            }
            for index in range(12)
        ],
    }


@durable_callable
async def summarize_order(order: dict[str, Any]) -> dict[str, Any]:
    """Summarize an order restored from its filesystem checkpoint."""
    items = order["items"]
    return {
        "order_id": order["order_id"],
        "item_count": len(items),
        "total_quantity": sum(item["quantity"] for item in items),
        "first_sku": items[0]["sku"],
        "last_sku": items[-1]["sku"],
    }


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Round-trip file-backed step results across a durable replay boundary."""
    serdes = JsonSerDes[dict[str, Any]]().then(
        FileSystemSerDesStage(
            event["mount_path"],
            FileSystemSerDesStageConfig(
                preview_config=PreviewConfig(
                    mode=PreviewMode.INCLUDE_ALL,
                    exclude=(PreviewField("items"),),
                    mask=(PreviewField("customer_email"),),
                )
            ),
        )
    )

    order = await step(
        build_order(event["order_id"]),
        name="persist-order",
        serdes=serdes,
    )

    # The next invocation must replay persist-order and load its result from the file.
    await wait(
        duration=timedelta(seconds=1),
        name="filesystem-replay-boundary",
    )

    summary = await step(
        summarize_order(order),
        name="summarize-order",
        serdes=serdes,
    )
    return {
        "success": True,
        "order_id": summary["order_id"],
        "item_count": summary["item_count"],
        "total_quantity": summary["total_quantity"],
        "first_sku": summary["first_sku"],
        "last_sku": summary["last_sku"],
    }

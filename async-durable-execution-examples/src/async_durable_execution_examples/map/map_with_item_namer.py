"""Example demonstrating map operations with custom iteration naming."""

import asyncio
from typing import Any

from async_durable_execution import (
    durable_callable,
    step,
    MapConfig,
    durable_execution,
    map,
)


@durable_execution
async def handler(_event: Any) -> list[str]:
    """Process orders using map() with custom iteration names."""
    orders = [
        {"id": "order-101", "amount": 25},
        {"id": "order-102", "amount": 50},
        {"id": "order-103", "amount": 75},
    ]

    async def process_order(order: dict[str, Any]) -> str:
        await asyncio.sleep(0)

        @durable_callable
        async def build_result() -> str:
            return f"processed-{order['id']}-${order['amount']}"

        return await step(build_result(), name=f"process_{order['id']}")

    return (
        await map(
            inputs=orders,
            func=process_order,
            name="process_orders",
            config=MapConfig(
                max_concurrency=2,
                item_namer=lambda order, index: f"order-{order['id']}",
            ),
        )
    ).get_results()

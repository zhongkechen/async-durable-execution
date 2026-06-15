"""Example demonstrating map operations with custom iteration naming."""

import asyncio
from typing import Any

from async_durable_execution.config import MapConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> list[str]:
    """Process orders using context.map() with custom iteration names."""
    orders = [
        {"id": "order-101", "amount": 25},
        {"id": "order-102", "amount": 50},
        {"id": "order-103", "amount": 75},
    ]

    async def process_order(
        ctx: DurableContext, order: dict[str, Any], index: int, _
    ) -> str:
        await asyncio.sleep(0)

        async def build_result() -> str:
            return f"processed-{order['id']}-${order['amount']}"

        return await ctx.step(build_result, name=f"process_{order['id']}")

    return (
        await context.map(
            inputs=orders,
            func=process_order,
            name="process_orders",
            config=MapConfig(
                max_concurrency=2,
                item_namer=lambda order, index: f"order-{order['id']}",
            ),
        )
    ).get_results()

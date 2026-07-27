"""Example showing multiple durable steps awaited with asyncio.gather."""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

from async_durable_execution import durable_callable, durable_execution, step


class LineItem(TypedDict):
    sku: str
    quantity: int
    unit_price: int


class PricedLineItem(TypedDict):
    sku: str
    quantity: int
    line_total: int


DEFAULT_ITEMS: list[LineItem] = [
    {"sku": "keyboard", "quantity": 1, "unit_price": 129},
    {"sku": "mouse", "quantity": 2, "unit_price": 45},
    {"sku": "monitor", "quantity": 1, "unit_price": 320},
]


@durable_callable
async def price_line_item(item: LineItem) -> PricedLineItem:
    return {
        "sku": item["sku"],
        "quantity": item["quantity"],
        "line_total": item["quantity"] * item["unit_price"],
    }


@durable_execution
async def handler(event: dict[str, Any] | None) -> dict[str, Any]:
    """Start several step tasks and await them together with asyncio.gather."""
    items = (event or {}).get("items", DEFAULT_ITEMS)

    pricing_tasks = [
        step(price_line_item(item), name=f"price-{item['sku']}") for item in items
    ]
    priced_items = await asyncio.gather(*pricing_tasks)

    return {
        "items": priced_items,
        "total": sum(item["line_total"] for item in priced_items),
    }

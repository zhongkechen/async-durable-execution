"""Example using a synchronous durable execution handler."""

from typing import Any

from async_durable_execution import durable_execution


@durable_execution
def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Handle an invocation with a regular synchronous function."""
    quantity = int(event.get("quantity", 1))
    unit_price = float(event.get("unit_price", 10.0))
    return {
        "order_id": str(event.get("order_id", "order-123")),
        "quantity": quantity,
        "total": round(quantity * unit_price, 2),
    }

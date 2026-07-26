"""Invoke another durable Lambda function and use the returned result."""

from typing import Any

from async_durable_execution import durable_execution, invoke


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    child_result: dict[str, Any] = await invoke(
        function_name=event["child_function_name"],
        payload={"orderId": event["order_id"]},
        name="price-order",
        tenant_id=event.get("tenant_id"),
    )

    return {
        "orderId": event["order_id"],
        "price": child_result["price"],
        "currency": child_result["currency"],
    }

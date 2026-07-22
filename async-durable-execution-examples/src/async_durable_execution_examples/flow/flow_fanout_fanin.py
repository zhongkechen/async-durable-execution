"""Build a fan-out/fan-in order workflow with declarative dependencies."""

from typing import Any, cast

from async_durable_execution import (
    FlowNode,
    durable_dag,
    durable_execution,
    durable_node,
    flow,
    node,
)


@durable_node
async def load_order(
    order_id: str,
    quantity: int,
    unit_price: float,
) -> dict[str, Any]:
    """Load the order data used by downstream branches."""
    return {
        "orderId": order_id,
        "quantity": quantity,
        "unitPrice": unit_price,
    }


@durable_node
async def validate_order(order_node: FlowNode[dict[str, Any]]) -> dict[str, Any]:
    """Validate an order after it has loaded."""
    order = order_node.outcome
    return {
        "orderId": order["orderId"],
        "valid": order["quantity"] > 0,
    }


@durable_node
async def price_order(order_node: FlowNode[dict[str, Any]]) -> float:
    """Calculate the order total in parallel with validation."""
    order = order_node.outcome
    return cast(float, order["quantity"] * order["unitPrice"])


@durable_node
async def build_response(
    validation_node: FlowNode[dict[str, Any]],
    pricing_node: FlowNode[float],
) -> dict[str, Any]:
    """Combine both fan-out branches after they complete."""
    validation = validation_node.outcome
    total = pricing_node.outcome
    return {
        "orderId": validation["orderId"],
        "valid": validation["valid"],
        "total": total,
    }


@durable_dag
def order_flow(
    order_id: str,
    quantity: int,
    unit_price: float,
) -> FlowNode[dict[str, Any]]:
    """Define a diamond-shaped order DAG."""
    order = node(
        load_order(order_id, quantity, unit_price),
        name="load-order",
    )
    validation = node(validate_order(order), name="validate-order")
    pricing = node(price_order(order), name="price-order")
    response = node(
        build_response(validation, pricing),
        name="build-response",
    )

    order >> (validation, pricing)
    (validation & pricing) >> response
    return response


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Run the fan-out/fan-in order flow."""
    result = await flow(
        order_flow(
            order_id=str(event["orderId"]),
            quantity=int(event["quantity"]),
            unit_price=float(event["unitPrice"]),
        ),
        name="order-processing-flow",
    )
    return cast(dict[str, Any], result.get_result("build-response").outcome)

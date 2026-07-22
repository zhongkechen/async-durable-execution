"""Route a flow through success or failure dependencies."""

from typing import Any, cast

from async_durable_execution import (
    FlowNode,
    FlowNodeContext,
    durable_dag,
    durable_execution,
    durable_node,
    flow,
    get_current_context,
    node,
)


@durable_node
async def charge_payment(approved: bool) -> str:
    """Simulate a payment attempt."""
    if not approved:
        msg = "payment declined"
        raise ValueError(msg)
    return "payment accepted"


@durable_node
async def fulfill_order(
    order_id: str,
    payment_node: FlowNode[str],
) -> str:
    """Run only when payment succeeds."""
    context = cast(FlowNodeContext, get_current_context())
    payment = context.result(payment_node)
    return f"fulfilled:{order_id}:{payment.outcome}"


@durable_node
async def record_payment_failure(payment_node: FlowNode[str]) -> str:
    """Handle a failed payment and expose its captured error."""
    context = cast(FlowNodeContext, get_current_context())
    payment = context.result(payment_node)
    message = payment.error.message if payment.error is not None else "unknown"
    return f"recovered:{message}"


@durable_dag
def payment_flow(
    order_id: str,
    approved: bool,
) -> tuple[FlowNode[str], FlowNode[str]]:
    """Define mutually exclusive success and recovery branches."""
    payment = node(charge_payment(approved), name="charge-payment")
    fulfillment = node(
        fulfill_order(order_id, payment),
        name="fulfill-order",
    )
    recovery = node(
        record_payment_failure(payment),
        name="record-payment-failure",
    )

    payment.succeeded >> fulfillment
    payment.failed >> recovery
    return fulfillment, recovery


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Run a payment flow and return every logical node result."""
    result = await flow(
        payment_flow(
            order_id=str(event["orderId"]),
            approved=bool(event["approved"]),
        ),
        name="payment-flow",
    )
    return result.to_dict()

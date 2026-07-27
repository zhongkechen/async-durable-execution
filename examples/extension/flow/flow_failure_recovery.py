"""Route a flow through success or failure dependencies."""

from typing import Any

from async_durable_execution import (
    ErrorObject,
    FlowNodeResult,
    durable_dag,
    durable_execution,
    durable_node,
    flow,
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
    payment: str,
) -> str:
    """Run only when payment succeeds."""
    return f"fulfilled:{order_id}:{payment}"


@durable_node
async def record_payment_failure(error: ErrorObject | None) -> str:
    """Handle a failed payment and expose its captured error."""
    message = error.message if error is not None else "unknown"
    return f"recovered:{message}"


@durable_dag
def payment_flow(
    order_id: str,
    approved: bool,
) -> tuple[FlowNodeResult[str], FlowNodeResult[str]]:
    """Define mutually exclusive success and recovery branches."""
    payment = node(charge_payment(approved), name="charge-payment")
    fulfillment = node(
        fulfill_order(order_id, payment.outcome),
        name="fulfill-order",
    )
    recovery = node(
        record_payment_failure(payment.error),
        name="record-payment-failure",
    )

    return fulfillment.result, recovery.result


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

"""Combine inferred inputs with nested conditional flow dependencies."""

from typing import Any, cast

from async_durable_execution import (
    FlowNodeStatus,
    durable_dag,
    durable_execution,
    durable_node,
    flow,
    get_node_context,
    node,
)


@durable_node
async def charge_payment(approved: bool) -> str:
    if not approved:
        msg = "payment declined"
        raise ValueError(msg)
    return "payment accepted"


@durable_node
async def reserve_inventory(available: bool) -> str:
    if not available:
        msg = "inventory unavailable"
        raise ValueError(msg)
    return "inventory reserved"


@durable_node
async def screen_risk(clear: bool) -> str:
    if not clear:
        msg = "risk review required"
        raise ValueError(msg)
    return "risk clear"


@durable_node
async def fulfill_order(
    order_id: str,
    payment: str,
    inventory: str,
    risk: str,
) -> str:
    return f"fulfilled:{order_id}:{payment}:{inventory}:{risk}"


@durable_node
async def recover_order() -> str:
    context = get_node_context()
    for name in ("charge-payment", "reserve-inventory", "screen-risk"):
        result = context.get_dependency_result(name)
        if (
            result is not None
            and result.status is FlowNodeStatus.FAILED
            and result.error is not None
        ):
            return f"recovered:{name}:{result.error.message}"
    msg = "Recovery started without an available failed dependency."
    raise RuntimeError(msg)


@durable_node
async def record_decision() -> dict[str, str]:
    context = get_node_context()
    fulfillment = context.get_dependency_result("fulfill-order")
    recovery = context.get_dependency_result("recover-order")
    risk = context.require_dependency_result("screen-risk")

    selected = next(
        (
            result
            for result in (fulfillment, recovery)
            if result is not None and result.status is FlowNodeStatus.SUCCEEDED
        ),
        None,
    )
    if selected is None:
        msg = "No completed order route is available."
        raise RuntimeError(msg)
    return {
        "decision": cast(str, selected.outcome),
        "riskStatus": risk.status.value,
    }


@durable_dag
def order_flow(
    order_id: str,
    *,
    payment_approved: bool,
    inventory_available: bool,
    risk_clear: bool,
) -> dict[str, str]:
    payment = node(charge_payment(payment_approved), name="charge-payment")
    inventory = node(
        reserve_inventory(inventory_available),
        name="reserve-inventory",
    )
    risk = node(screen_risk(risk_clear), name="screen-risk")

    fulfillment = node(
        fulfill_order(
            order_id,
            payment.outcome,
            inventory.outcome,
            risk.outcome,
        ),
        name="fulfill-order",
    )
    recovery = node(
        recover_order(),
        name="recover-order",
        dependency=payment.failed | inventory.failed | risk.failed,
    )
    decision = node(
        record_decision(),
        name="record-decision",
        dependency=(fulfillment.succeeded | recovery.succeeded) & risk.completed,
    )
    return decision.outcome


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, str]:
    result = await flow(
        order_flow(
            str(event["orderId"]),
            payment_approved=bool(event["paymentApproved"]),
            inventory_available=bool(event["inventoryAvailable"]),
            risk_clear=bool(event["riskClear"]),
        ),
        name="complex-order-flow",
    )
    return cast(dict[str, str], result.output)

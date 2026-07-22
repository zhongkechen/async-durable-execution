"""Use durable step and wait operations inside flow nodes."""

from datetime import timedelta
from typing import Any, cast

from async_durable_execution import (
    FlowNode,
    FlowNodeContext,
    durable_callable,
    durable_dag,
    durable_execution,
    durable_node,
    flow,
    get_current_context,
    node,
    step,
    wait,
)


@durable_callable
async def fetch_customer(customer_id: str) -> dict[str, str]:
    """Represent a side-effecting customer lookup."""
    return {"customerId": customer_id, "email": f"{customer_id}@example.com"}


@durable_callable
async def format_notification(customer: dict[str, str]) -> str:
    """Represent durable notification preparation."""
    return f"notification-ready:{customer['email']}"


@durable_node
async def load_customer(customer_id: str) -> dict[str, str]:
    """Run a durable step inside a flow node."""
    return await step(
        fetch_customer(customer_id),
        name="fetch-customer",
    )


@durable_node
async def prepare_notification(
    customer_node: FlowNode[dict[str, str]],
) -> str:
    """Wait durably before running another step in the node."""
    context = cast(FlowNodeContext, get_current_context())
    customer = cast(
        dict[str, str],
        context.result(customer_node).outcome,
    )
    await wait(
        duration=timedelta(seconds=1),
        name="notification-delay",
    )
    return await step(
        format_notification(customer),
        name="format-notification",
    )


@durable_dag
def notification_flow(customer_id: str) -> FlowNode[str]:
    """Define a linear flow whose nodes contain durable operations."""
    customer = node(load_customer(customer_id), name="load-customer")
    notification = node(
        prepare_notification(customer),
        name="prepare-notification",
    )
    customer >> notification
    return notification


@durable_execution
async def handler(event: dict[str, Any]) -> str:
    """Run the notification flow."""
    result = await flow(
        notification_flow(str(event["customerId"])),
        name="notification-flow",
    )
    return cast(str, result.get_result("prepare-notification").outcome)

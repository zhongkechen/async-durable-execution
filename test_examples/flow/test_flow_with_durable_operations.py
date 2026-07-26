"""Tests for durable operations nested inside flow nodes."""

from async_durable_execution import (
    InvocationStatus,
    OperationStatus,
    OperationType,
)
from examples.flow import flow_with_durable_operations


async def test_flow_with_durable_operations(durable_runner):
    """Verify step and wait operations use each node's child context."""
    async with durable_runner(
        handler=flow_with_durable_operations.handler,
        input={"customerId": "customer-123"},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert (
        result.get_deserialized_result()
        == "notification-ready:customer-123@example.com"
    )

    flow_operation = result.get_context("notification-flow")
    assert flow_operation is not None
    node_operations = {
        operation.name: operation
        for operation in result.get_child_operations(flow_operation)
    }
    assert set(node_operations) == {"load-customer", "prepare-notification"}
    assert all(
        operation.status is OperationStatus.SUCCEEDED
        for operation in node_operations.values()
    )

    load_operations = result.get_child_operations(node_operations["load-customer"])
    assert [
        (operation.operation_type, operation.name) for operation in load_operations
    ] == [(OperationType.STEP, "fetch-customer")]

    notification_operations = result.get_child_operations(
        node_operations["prepare-notification"]
    )
    assert {
        (operation.operation_type, operation.name)
        for operation in notification_operations
    } == {
        (OperationType.WAIT, "notification-delay"),
        (OperationType.STEP, "format-notification"),
    }

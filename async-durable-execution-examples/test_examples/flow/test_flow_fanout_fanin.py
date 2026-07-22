"""Tests for the fan-out/fan-in flow example."""

from async_durable_execution import InvocationStatus, OperationStatus
from async_durable_execution_examples.flow import flow_fanout_fanin


async def test_flow_fanout_fanin(durable_runner):
    """Run two dependent branches and join their results."""
    async with durable_runner(
        handler=flow_fanout_fanin.handler,
        input={
            "orderId": "order-123",
            "quantity": 3,
            "unitPrice": 12.5,
        },
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "orderId": "order-123",
        "valid": True,
        "total": 37.5,
    }

    flow_operation = result.get_context("order-processing-flow")
    assert flow_operation is not None
    assert flow_operation.status is OperationStatus.SUCCEEDED
    node_operations = result.get_child_operations(flow_operation)
    assert [operation.name for operation in node_operations] == [
        "load-order",
        "validate-order",
        "price-order",
        "build-response",
    ]
    assert all(
        operation.status is OperationStatus.SUCCEEDED for operation in node_operations
    )

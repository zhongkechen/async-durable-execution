"""Tests for map_operations example."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution.lambda_service import (
    OperationStatus,
)
from async_durable_execution_examples.map import map_operations


async def test_map_operations(durable_runner):
    """Test map_operations example using context.map()."""
    with durable_runner(
        handler=map_operations.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [2, 4, 6, 8, 10]

    # Get the map operation (CONTEXT type with MAP subtype)
    map_op = result.get_context("map_operation")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all five child operations exist
    assert len(map_op.child_operations) == 5

    # Verify child operation names (SDK uses map-item-* format)
    child_names = {op.name for op in map_op.child_operations}
    expected_names = {f"map-item-{i}" for i in range(5)}
    assert child_names == expected_names

    # Verify all children succeeded
    for child in map_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED

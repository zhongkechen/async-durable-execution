"""Tests for map_operations example."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationStatus
from examples.extension.map import map_operations_flat


async def test_map_operations_flat(durable_runner):
    """Test map_operations example using map()."""
    async with durable_runner(
        handler=map_operations_flat.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [2, 4, 6, 8, 10]

    # Get the map operation (CONTEXT type with MAP subtype)
    map_op = result.get_context("map_operation")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all five child operations exist
    assert len(result.get_child_operations(map_op)) == 5

    # Verify child step operation names
    child_names = {op.name for op in result.get_child_operations(map_op)}
    expected_names = {f"map_item_{i}" for i in range(5)}
    assert child_names == expected_names

    # Verify all children succeeded
    for child in result.get_child_operations(map_op):
        assert child.status is OperationStatus.SUCCEEDED

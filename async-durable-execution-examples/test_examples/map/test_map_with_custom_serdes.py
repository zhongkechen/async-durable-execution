"""Tests for map with custom serdes."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationStatus
from async_durable_execution_examples.map import map_with_custom_serdes


async def test_map_with_custom_serdes(durable_runner):
    """Test map with custom item serialization."""
    async with durable_runner(
        handler=map_with_custom_serdes.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    # Verify all items were processed
    assert result_data["success_count"] == 3

    # Verify results were properly deserialized
    results = result_data["results"]
    assert len(results) == 3

    # Verify the custom serdes worked (data was serialized and deserialized correctly)
    processed_names = result_data["processed_names"]
    assert processed_names == ["item1", "item2", "item3"]

    # Verify processing logic worked correctly
    for i, r in enumerate(results):
        assert r["index"] == i
        assert r["doubled_id"] == (i + 1) * 2  # IDs are 1, 2, 3

    # Get the map operation
    map_op = result.get_context("map_with_custom_serdes")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all 3 child operations exist and succeeded
    assert len(result.get_child_operations(map_op)) == 3
    for child in result.get_child_operations(map_op):
        assert child.status is OperationStatus.SUCCEEDED

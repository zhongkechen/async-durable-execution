"""Tests for map with maxConcurrency."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationStatus
from async_durable_execution_examples.map import map_with_max_concurrency


async def test_map_with_max_concurrency(durable_runner):
    """Test map with maxConcurrency limit."""
    async with durable_runner(
        handler=map_with_max_concurrency.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    results_list = result.get_deserialized_result()
    assert len(results_list) == 10
    # Items 1-10 multiplied by 3
    assert results_list == [3, 6, 9, 12, 15, 18, 21, 24, 27, 30]

    # Get the map operation
    map_op = result.get_context("map_with_concurrency")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all 10 child operations exist
    assert len(result.get_child_operations(map_op)) == 10

    # Verify all children succeeded
    for child in result.get_child_operations(map_op):
        assert child.status is OperationStatus.SUCCEEDED

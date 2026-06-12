"""Tests for map with batch-level serdes."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution.lambda_service import OperationStatus
from async_durable_execution_examples.map import map_with_batch_serdes


def test_map_with_batch_serdes(durable_runner):
    """Test map with custom batch-level serialization."""
    with durable_runner(
        handler=map_with_batch_serdes.handler, input="test", timeout=10
    ) as runner:
        result = runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    # Verify all items were processed
    assert result_data["success_count"] == 4

    # Verify results
    results = result_data["results"]
    assert len(results) == 4
    assert results == [20, 40, 60, 80]  # [10*2, 20*2, 30*2, 40*2]

    # Verify sum
    assert result_data["sum"] == 200

    # Get the map operation
    map_op = result.get_context("map_with_batch_serdes")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all 4 child operations exist and succeeded
    assert len(map_op.child_operations) == 4
    for child in map_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED

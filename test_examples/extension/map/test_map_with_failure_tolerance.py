"""Tests for map with failure tolerance."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationStatus
from examples.extension.map import map_with_failure_tolerance


async def test_map_with_failure_tolerance(durable_runner):
    """Test map with failure tolerance."""
    async with durable_runner(
        handler=map_with_failure_tolerance.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    # Should have 7 successes and 3 failures (items 3, 6, 9 fail)
    assert result_data["success_count"] == 7
    assert result_data["failure_count"] == 3
    assert result_data["failed_count"] == 3

    # Verify successful results (items 1,2,4,5,7,8,10 multiplied by 2)
    expected_results = [2, 4, 8, 10, 14, 16, 20]
    assert set(result_data["succeeded"]) == set(expected_results)

    assert result_data["completion_reason"] == "ALL_COMPLETED"

    # Get the map operation
    map_op = result.get_context("map_with_tolerance")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify all 10 child operations exist
    assert len(result.get_child_operations(map_op)) == 10

    # Count successes and failures
    succeeded = [
        op
        for op in result.get_child_operations(map_op)
        if op.status is OperationStatus.SUCCEEDED
    ]
    failed = [
        op
        for op in result.get_child_operations(map_op)
        if op.status is OperationStatus.FAILED
    ]

    assert len(succeeded) == 7
    assert len(failed) == 3

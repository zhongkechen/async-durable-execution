"""Tests for parallel with failure tolerance."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationStatus
from async_durable_execution_examples.parallel import parallel_with_failure_tolerance


async def test_parallel_with_failure_tolerance(durable_runner):
    """Test parallel with failure tolerance."""
    with durable_runner(
        handler=parallel_with_failure_tolerance.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    # Should have 3 successes and 2 failures
    assert result_data["success_count"] == 3
    assert result_data["failure_count"] == 2
    assert set(result_data["succeeded"]) == {"success 1", "success 3", "success 5"}
    assert result_data["completion_reason"] == "ALL_COMPLETED"

    # Get the parallel operation
    parallel_op = result.get_context("parallel_with_tolerance")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all 5 child operations exist
    assert len(result.get_child_operations(parallel_op)) == 5

    # Count successes and failures
    succeeded = [
        op
        for op in result.get_child_operations(parallel_op)
        if op.status is OperationStatus.SUCCEEDED
    ]
    failed = [
        op for op in result.get_child_operations(parallel_op) if op.status is OperationStatus.FAILED
    ]

    assert len(succeeded) == 3
    assert len(failed) == 2

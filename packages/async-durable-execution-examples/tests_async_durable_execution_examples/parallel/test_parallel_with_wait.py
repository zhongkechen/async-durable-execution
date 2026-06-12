"""Tests for parallel with wait operations."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution.lambda_service import (
    OperationStatus,
    OperationType,
)
from async_durable_execution_examples.parallel import parallel_with_wait


def test_parallel_with_wait(durable_runner):
    """Test parallel with wait operations."""
    with durable_runner(
        handler=parallel_with_wait.handler, input="test", timeout=10
    ) as runner:
        result = runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "Completed waits"

    # Get the parallel operation
    parallel_op = result.get_context("parallel_waits")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all 3 child operations exist
    assert len(parallel_op.child_operations) == 3

    # Each child should have a wait operation
    wait_names = set()
    for child in parallel_op.child_operations:
        # Find wait operations in child
        wait_ops = [
            op
            for op in child.child_operations
            if op.operation_type == OperationType.WAIT
        ]
        assert len(wait_ops) == 1
        wait_names.add(wait_ops[0].name)

    # Verify all expected wait operations exist
    assert wait_names == {"wait_1_second", "wait_2_seconds", "wait_5_seconds"}

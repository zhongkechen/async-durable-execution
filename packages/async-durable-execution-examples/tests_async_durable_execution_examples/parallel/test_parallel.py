"""Tests for parallel example."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution.lambda_service import (
    OperationStatus,
    OperationType,
)
from async_durable_execution_examples.parallel import parallel


def test_parallel(durable_runner):
    """Test parallel example using context.parallel()."""
    with durable_runner(handler=parallel.handler, input="test", timeout=10) as runner:
        result = runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [
        "task 1 completed",
        "task 2 completed",
        "task 3 completed after wait",
    ]

    # Get the parallel operation (CONTEXT type with PARALLEL subtype)
    parallel_op = result.get_context("parallel_operation")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all three child operations exist
    assert len(parallel_op.child_operations) == 3

    # Verify all children succeeded
    for child in parallel_op.child_operations:
        assert child.operation_type == OperationType.CONTEXT
        assert child.status is OperationStatus.SUCCEEDED

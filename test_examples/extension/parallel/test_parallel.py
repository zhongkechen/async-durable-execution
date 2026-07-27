"""Tests for parallel example."""

from async_durable_execution import InvocationStatus
from async_durable_execution import (
    OperationStatus,
    OperationType,
)
from examples.extension.parallel import parallel


async def test_parallel(durable_runner):
    """Test parallel example using parallel()."""
    async with durable_runner(
        handler=parallel.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

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
    assert len(result.get_child_operations(parallel_op)) == 3

    # Verify all children succeeded
    for child in result.get_child_operations(parallel_op):
        assert child.operation_type == OperationType.CONTEXT
        assert child.status is OperationStatus.SUCCEEDED

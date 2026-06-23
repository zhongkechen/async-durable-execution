"""Tests for parallel_with_bound_callables example."""

from async_durable_execution import InvocationStatus
from async_durable_execution import (
    OperationStatus,
    OperationType,
)
from async_durable_execution_examples.parallel import parallel_with_bound_callables


async def test_parallel_with_bound_callables(durable_runner):
    """Test parallel example with bound durable callables."""
    with durable_runner(
        handler=parallel_with_bound_callables.handler,
        input={"user_id": "user-456"},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [
        "user-data-loaded-user-456",
        "orders-loaded-user-456",
        "prefs-loaded-user-456",
        "usage-metrics-loaded-user-456",
        "config-loaded-us-east-1",
    ]

    parallel_op = result.get_context("load_all_data")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED
    assert len(parallel_op.child_operations) == 5

    child_names = [op.name for op in parallel_op.child_operations]
    assert child_names == [
        "parallel-branch-0",
        "parallel-branch-1",
        "parallel-branch-2",
        "parallel-branch-3",
        "parallel-branch-4",
    ]

    for child in parallel_op.child_operations:
        assert child.operation_type == OperationType.CONTEXT
        assert child.status is OperationStatus.SUCCEEDED

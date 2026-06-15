"""Tests for parallel_with_named_branches example."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution.models import (
    OperationStatus,
    OperationType,
)
from async_durable_execution_examples.parallel import parallel_with_named_branches


async def test_parallel_with_named_branches(durable_runner):
    """Test parallel example with all branch patterns."""
    with durable_runner(
        handler=parallel_with_named_branches.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [
        "user-data-loaded",
        "orders-loaded",
        "prefs-loaded",
        "metrics-loaded",
        "config-loaded",
    ]

    # Get the parallel operation
    parallel_op = result.get_context("load_all_data")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify branch names: named branches have custom names, unnamed use defaults
    assert len(parallel_op.child_operations) == 5

    child_names = [op.name for op in parallel_op.child_operations]

    # 1. Named ParallelBranch
    assert child_names[0] == "fetch-user-data"
    # 2. Named decorator
    assert child_names[1] == "fetch-orders"
    # 3. Unnamed decorator (None name falls back to index-based default)
    assert child_names[2] == "parallel-branch-2"
    # 4. Unnamed ParallelBranch (None name falls back to index-based default)
    assert child_names[3] == "parallel-branch-3"
    # 5. Raw callable (no ParallelBranch wrapper, index-based default)
    assert child_names[4] == "parallel-branch-4"

    # Verify all children succeeded
    for child in parallel_op.child_operations:
        assert child.operation_type == OperationType.CONTEXT
        assert child.status is OperationStatus.SUCCEEDED

"""Tests for parallel with batch-level serdes."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution.models import OperationStatus
from async_durable_execution_examples.parallel import parallel_with_batch_serdes


async def test_parallel_with_batch_serdes(durable_runner):
    """Test parallel with custom batch-level serialization."""
    with durable_runner(
        handler=parallel_with_batch_serdes.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    # Verify all branches succeeded
    assert result_data["success_count"] == 3

    # Verify results
    results = result_data["results"]
    assert len(results) == 3
    assert results == [100, 200, 300]

    # Verify total
    assert result_data["total"] == 600

    # Get the parallel operation
    parallel_op = result.get_context("parallel_with_batch_serdes")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all 3 child operations exist and succeeded
    assert len(parallel_op.child_operations) == 3
    for child in parallel_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED

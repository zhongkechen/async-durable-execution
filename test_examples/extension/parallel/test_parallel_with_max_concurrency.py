"""Tests for parallel with maxConcurrency."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationStatus
from examples.extension.parallel import parallel_with_max_concurrency


async def test_parallel_with_max_concurrency(durable_runner):
    """Test parallel with maxConcurrency limit."""
    async with durable_runner(
        handler=parallel_with_max_concurrency.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    results_list = result.get_deserialized_result()
    assert len(results_list) == 5
    assert set(results_list) == {"task 1", "task 2", "task 3", "task 4", "task 5"}

    # Get the parallel operation
    parallel_op = result.get_context("parallel_with_concurrency")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all 5 child operations exist
    assert len(result.get_child_operations(parallel_op)) == 5

    # Verify all children succeeded
    for child in result.get_child_operations(parallel_op):
        assert child.status is OperationStatus.SUCCEEDED

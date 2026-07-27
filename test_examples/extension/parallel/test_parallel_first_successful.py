"""Tests for parallel first successful example."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationStatus
from examples.extension.parallel import parallel_first_successful


async def test_parallel_first_successful(durable_runner):
    """Test parallel with first_successful completion strategy."""
    async with durable_runner(
        handler=parallel_first_successful.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()
    # The handler returns a string like "First successful result: Task 1"
    assert result_data.startswith("First successful result: ")
    # The first successful result should be one of the tasks
    assert result_data in [
        "First successful result: Task 1",
        "First successful result: Task 2",
        "First successful result: Task 3",
    ]

    # Get the parallel operation
    parallel_op = result.get_context("first_successful_parallel")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify child operations exist (3 branches)
    assert len(result.get_child_operations(parallel_op)) == 3

    # At least one child should have succeeded
    succeeded = [
        op
        for op in result.get_child_operations(parallel_op)
        if op.status is OperationStatus.SUCCEEDED
    ]
    assert len(succeeded) >= 1

"""Tests for parallel with custom serdes."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationStatus
from async_durable_execution_examples.parallel import parallel_with_custom_serdes


async def test_parallel_with_custom_serdes(durable_runner):
    """Test parallel with custom item serialization."""
    with durable_runner(
        handler=parallel_with_custom_serdes.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    # Verify all tasks succeeded
    assert result_data["success_count"] == 3

    # Verify results were properly deserialized
    results = result_data["results"]
    assert len(results) == 3

    # Verify the custom serdes worked (data was serialized and deserialized correctly)
    task_names = {r["task"] for r in results}
    assert task_names == {"task1", "task2", "task3"}

    # Verify values were preserved through serialization
    assert result_data["total_value"] == 600  # 100 + 200 + 300

    # Get the parallel operation
    parallel_op = result.get_context("parallel_with_custom_serdes")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all 3 child operations exist and succeeded
    assert len(parallel_op.child_operations) == 3
    for child in parallel_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED

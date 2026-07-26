"""Tests for undefined_results."""

from async_durable_execution import InvocationStatus
from examples.none_results import none_results


async def test_handle_step_operations_with_undefined_result_after_replay(
    durable_runner,
):
    """Test handling of step operations with undefined result after replay."""
    async with durable_runner(
        handler=none_results.handler, input=None, timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    # Verify execution completed successfully despite undefined operation results
    assert result.get_deserialized_result() == "result"

    # Verify all operations were tracked even with undefined results
    operations = result.operations
    assert len(operations) == 3  # step + context + wait

    # Verify step operation with undefined result
    step_op = result.get_step("fetch-user")
    assert result.get_operation_deserialized_result(step_op) is None

    # Verify child context operation with undefined result
    context_op = result.get_context("parent")
    assert result.get_operation_deserialized_result(context_op) is None

    # Verify wait operation completed normally
    wait_op = operations[2]
    assert wait_op.operation_type.value == "WAIT"

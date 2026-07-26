"""Tests for wait_for_callback_mixed_ops."""

import json

from async_durable_execution import InvocationStatus
from examples.wait_for_callback import (
    wait_for_callback_mixed_ops,
)


async def test_handle_wait_for_callback_mixed_with_steps_waits_and_other_operations(
    durable_runner,
):
    """Test waitForCallback mixed with steps, waits, and other operations."""
    async with durable_runner(
        handler=wait_for_callback_mixed_ops.handler, input=None, timeout=30
    ) as runner:
        # Start the execution (this will pause at the callback)
        execution_arn = await runner.run_async()

        # Wait for callback and get callback_id
        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)

        # Complete the callback
        callback_result = json.dumps({"processed": True})
        await runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )

        # Wait for the execution to complete
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    # Verify all expected fields
    assert result_data["stepResult"] == {"userId": 123, "name": "John Doe"}
    assert result_data["callbackResult"] == callback_result
    assert result_data["finalStep"]["status"] == "completed"
    assert isinstance(result_data["finalStep"]["timestamp"], int)
    assert result_data["workflowCompleted"] is True

    # Verify all operations were tracked - should have wait, step, waitForCallback (context + callback + submitter), wait, step
    completed_operations = [
        op for op in result.get_all_operations() if op.status.value == "SUCCEEDED"
    ]
    assert len(completed_operations) == 7

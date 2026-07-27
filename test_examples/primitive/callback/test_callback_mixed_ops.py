"""Tests for create_callback_mixed_ops."""

import json

from async_durable_execution import InvocationStatus
from examples.primitive.callback import callback_mixed_ops


async def test_handle_callback_operations_mixed_with_other_operation_types(
    durable_runner,
):
    """Test callback operations mixed with other operation types."""
    async with durable_runner(
        handler=callback_mixed_ops.handler, input=None, timeout=30
    ) as runner:
        execution_arn = await runner.run_async()
        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)
        callback_result = json.dumps(
            {
                "processed": True,
            }
        )
        await runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    assert result_data == {
        "stepResult": {"userId": 123, "name": "John Doe"},
        "callbackResult": callback_result,
        "completed": True,
    }

    completed_operations = result.operations
    assert len(completed_operations) == 3

    operation_types = [op.operation_type.value for op in completed_operations]
    assert "WAIT" in operation_types
    assert "STEP" in operation_types
    assert "CALLBACK" in operation_types

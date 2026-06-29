"""Tests for create_callback_concurrent."""

import json

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.callback import callback_concurrency


async def test_handle_multiple_concurrent_callback_operations(durable_runner):
    """Test handling multiple concurrent callback operations."""
    with durable_runner(
        handler=callback_concurrency.handler, input=None, timeout=60
    ) as runner:
        # Start the execution (this will pause at the callbacks)
        execution_arn = await runner.run_async()

        callback_id_1 = await runner.wait_for_callback(
            execution_arn=execution_arn, name="api-call-1"
        )
        callback_id_2 = await runner.wait_for_callback(
            execution_arn=execution_arn, name="api-call-2"
        )
        callback_id_3 = await runner.wait_for_callback(
            execution_arn=execution_arn, name="api-call-3"
        )

        callback_result_2 = json.dumps(
            {
                "id": 2,
                "data": "second",
            }
        )
        await runner.send_callback_success(
            callback_id=callback_id_2, result=callback_result_2.encode()
        )

        callback_result_1 = json.dumps(
            {
                "id": 1,
                "data": "first",
            }
        )
        await runner.send_callback_success(
            callback_id=callback_id_1, result=callback_result_1.encode()
        )

        callback_result_3 = json.dumps(
            {
                "id": 3,
                "data": "third",
            }
        )
        await runner.send_callback_success(
            callback_id=callback_id_3, result=callback_result_3.encode()
        )

        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    assert result_data == {
        "results": [callback_result_1, callback_result_2, callback_result_3],
        "allCompleted": True,
    }

    # Verify all callback operations were tracked
    operations = result.get_context("parallel_callbacks")

    assert len(result.get_child_operations(operations)) == 3

    # Verify all operations are CALLBACK type
    for op in result.get_child_operations(operations):
        assert op.operation_type.value == "CONTEXT"
        assert len(result.get_child_operations(op)) == 1
        assert result.get_child_operations(op)[0].operation_type.value == "CALLBACK"

"""Tests for wait_for_callback_anonymous."""

import json

from async_durable_execution import InvocationStatus
from examples.extension.wait_for_callback import (
    wait_for_callback_anonymous,
)


async def test_handle_basic_wait_for_callback_with_anonymous_submitter(durable_runner):
    """Test basic waitForCallback with anonymous submitter."""
    async with durable_runner(
        handler=wait_for_callback_anonymous.handler, input=None, timeout=30
    ) as runner:
        execution_arn = await runner.run_async()
        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)
        callback_result = json.dumps({"data": "callback_completed"})
        await runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )

        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    assert result_data == {
        "callbackResult": callback_result,
        "completed": True,
    }

    # Verify operations were tracked
    assert len(result.operations) > 0

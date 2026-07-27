"""Tests for wait_for_callback_heartbeat_sends."""

import asyncio
import json

from async_durable_execution import InvocationStatus
from examples.extension.wait_for_callback import (
    wait_for_callback_heartbeat,
)


async def test_handle_wait_for_callback_heartbeat_scenarios_during_long_running_submitter(
    durable_runner,
):
    """Test waitForCallback heartbeat scenarios during long-running submitter execution."""

    async with durable_runner(
        handler=wait_for_callback_heartbeat.handler,
        input=None,
        timeout=60,
        time_scale="1.0",
    ) as runner:
        # Start the execution (this will pause at the callback)
        execution_arn = await runner.run_async()

        # Wait for callback and get callback_id
        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)

        # Send heartbeat to keep the callback alive during processing
        await runner.send_callback_heartbeat(callback_id=callback_id)

        # Wait a bit more to simulate callback processing time
        wait_time = 0.2
        await asyncio.sleep(wait_time)

        # Send another heartbeat
        await runner.send_callback_heartbeat(callback_id=callback_id)

        # Finally complete the callback
        callback_result = json.dumps({"processed": 1000})
        await runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )

        # Wait for the execution to complete
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    assert result_data["callbackResult"] == callback_result
    assert result_data["completed"] is True

    # Should have completed operations with successful callback
    completed_operations = [
        op for op in result.operations if op.status.value == "SUCCEEDED"
    ]
    assert len(completed_operations) > 0

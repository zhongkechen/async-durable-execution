"""Tests for create_callback_heartbeat."""

import json
import time

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.callback import callback_heartbeat


async def test_handle_callback_operations_with_failure_uncaught(durable_runner):
    """Test handling callback operations with failure."""
    test_payload = {"shouldCatchError": False}

    heartbeat_interval = 5
    total_duration = 20
    num_heartbeats = total_duration // heartbeat_interval

    with durable_runner(
        handler=callback_heartbeat.handler, input=test_payload, timeout=30
    ) as runner:
        execution_arn = await runner.run_async()

        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)

        for i in range(num_heartbeats):
            print(
                f"Sending heartbeat {i + 1}/{num_heartbeats} at {(i + 1) * heartbeat_interval}s"
            )
            await runner.send_callback_heartbeat(callback_id=callback_id)
            time.sleep(heartbeat_interval)

        callback_result = json.dumps(
            {
                "status": "completed",
                "data": "success after heartbeats",
            }
        )
        await runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )

        result = await runner.wait_for_result(execution_arn=execution_arn)
    assert result.status is InvocationStatus.SUCCEEDED

    # Assert the callback result is returned
    result_data = result.get_deserialized_result()
    assert result_data == callback_result

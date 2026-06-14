"""Tests for wait_for_callback_serdes."""

from datetime import UTC, datetime

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.wait_for_callback import wait_for_callback_serdes
from async_durable_execution_examples.wait_for_callback.wait_for_callback_serdes import (
    CustomSerdes,
)


async def test_handle_wait_for_callback_with_custom_serdes_configuration(
    durable_runner,
):
    """Test waitForCallback with custom serdes configuration."""
    with durable_runner(
        handler=wait_for_callback_serdes.handler, input=None, timeout=30
    ) as runner:
        # Start the execution (this will pause at the callback)
        execution_arn = await runner.run_async()

        # Wait for callback and get callback_id
        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)

        # Send data that requires custom serialization
        test_data = {
            "id": 42,
            "message": "Hello Custom Serdes",
            "timestamp": datetime(2025, 6, 15, 12, 30, 45, tzinfo=UTC),
            "metadata": {
                "version": "2.0.0",
                "processed": True,
            },
        }

        # Serialize the data using custom serdes for sending
        custom_serdes = CustomSerdes()
        serialized_data = custom_serdes.serialize(test_data)
        await runner.send_callback_success(
            callback_id=callback_id, result=serialized_data.encode()
        )

        # Wait for the execution to complete
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    # The result will always get stringified since it's the lambda response
    # DateTime will be serialized to ISO string in the final result
    assert result_data["receivedData"]["id"] == 42
    assert result_data["receivedData"]["message"] == "Hello Custom Serdes"
    assert "2025-06-15T12:30:45" in result_data["receivedData"]["timestamp"]
    assert result_data["receivedData"]["metadata"]["version"] == "2.0.0"
    assert result_data["receivedData"]["metadata"]["processed"] is True
    assert result_data["isDateObject"] is True

    # Should have completed operations with successful callback
    completed_operations = [
        op for op in result.operations if op.status.value == "SUCCEEDED"
    ]
    assert len(completed_operations) > 0

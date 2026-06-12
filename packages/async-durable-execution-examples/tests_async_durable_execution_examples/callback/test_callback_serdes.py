"""Tests for create_callback_serdes."""

from datetime import UTC, datetime

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.callback import callback_serdes
from async_durable_execution_examples.callback.callback_serdes import (
    CustomData,
    CustomDataSerDes,
)


class CustomDataTestSerDes(CustomDataSerDes):
    """Test version of CustomDataSerDes for use in tests."""


def test_handle_callback_operations_with_custom_serdes(durable_runner):
    """Test callback operations with custom serdes."""
    with durable_runner(
        handler=callback_serdes.handler, input=None, timeout=30
    ) as runner:
        # Start the execution (this will pause at the callback)
        execution_arn = runner.run_async()

        # Wait for callback and get callback_id
        callback_id = runner.wait_for_callback(execution_arn=execution_arn)

        # Send data that requires custom serialization
        test_data = CustomData(
            id=42,
            message="Hello World",
            timestamp=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
        )

        # Serialize the data using custom serdes for sending
        serdes = CustomDataTestSerDes()
        serialized_data = serdes.serialize(test_data, None)

        runner.send_callback_success(
            callback_id=callback_id, result=serialized_data.encode()
        )

        # Wait for the execution to complete
        result = runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    # Verify the result structure
    assert result_data["receivedData"]["id"] == 42
    assert result_data["receivedData"]["message"] == "Hello World"
    assert "2025-01-01T00:00:00" in result_data["receivedData"]["timestamp"]
    assert result_data["isDateObject"] is True

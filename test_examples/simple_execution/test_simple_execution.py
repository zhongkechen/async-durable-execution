"""Tests for simple_execution."""

from async_durable_execution import InvocationStatus
from examples.simple_execution import simple_execution


async def test_execute_simple_handler_without_operations(durable_runner) -> None:
    """Test simple handler execution without operations."""
    test_payload = {
        "userId": "test-user",
        "action": "simple-execution",
    }

    async with durable_runner(
        handler=simple_execution.handler, input=test_payload, timeout=10
    ) as runner:
        result = await runner.run()

    result_data = result.get_deserialized_result()

    # Verify the result structure and content
    assert (
        result_data["received"]
        == '{"userId": "test-user", "action": "simple-execution"}'
    )
    assert result_data["message"] == "Handler completed successfully"
    assert isinstance(result_data["timestamp"], int)
    assert result_data["timestamp"] > 0

    # Should have no operations for simple execution
    assert len(result.operations) == 0

    # Verify no error occurred
    assert result.status is InvocationStatus.SUCCEEDED

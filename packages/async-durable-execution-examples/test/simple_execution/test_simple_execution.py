"""Tests for simple_execution."""

import pytest
from async_durable_execution.execution import InvocationStatus

from async_durable_execution_examples.simple_execution import simple_execution
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=simple_execution.handler,
)
def test_execute_simple_handler_without_operations(durable_runner):
    """Test simple handler execution without operations."""
    test_payload = {
        "userId": "test-user",
        "action": "simple-execution",
    }

    with durable_runner:
        result = durable_runner.run(input=test_payload, timeout=10)

    result_data = deserialize_operation_payload(result.result)

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

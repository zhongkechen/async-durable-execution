"""Tests for handler_error."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.handler_error import handler_error


def test_handle_handler_errors_gracefully_and_capture_error_details(durable_runner):
    """Test that handler errors are handled gracefully and error details are captured."""
    test_payload = {"test": "error-case"}

    with durable_runner(
        handler=handler_error.handler, input=test_payload, timeout=10
    ) as runner:
        result = runner.run()

    # Verify execution failed
    assert result.status is InvocationStatus.FAILED

    # Check that error was captured in the result
    error = result.error
    assert error is not None

    assert error.message == "Intentional handler failure"
    assert error.type == "Exception"

    # Verify no operations were completed due to early error
    assert len(result.operations) == 0

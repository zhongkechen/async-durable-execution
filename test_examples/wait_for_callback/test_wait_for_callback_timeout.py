"""Tests for wait_for_callback_timeout."""

from async_durable_execution import InvocationStatus, OperationStatus, OperationType
from examples.wait_for_callback import wait_for_callback_timeout


async def test_handle_wait_for_callback_timeout_scenarios(durable_runner) -> None:
    """Test waitForCallback timeout scenarios."""
    test_payload = {"test": "timeout-scenario"}

    async with durable_runner(
        handler=wait_for_callback_timeout.handler, input=test_payload, timeout=30
    ) as runner:
        execution_arn = await runner.run_async()
        # Don't send callback - let it timeout
        result = await runner.wait_for_result(execution_arn=execution_arn)

    # Handler catches the timeout error, so execution succeeds with error in result
    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    assert result_data["success"] is False
    assert isinstance(result_data["error"], str)
    assert len(result_data["error"]) > 0
    assert result_data["error"] == "Callback timed out"
    callbacks = [
        operation
        for operation in result.get_all_operations()
        if operation.operation_type is OperationType.CALLBACK
    ]
    assert len(callbacks) == 1
    assert callbacks[0].status is OperationStatus.TIMED_OUT
    assert callbacks[0].callback_details.error.type == "Callback.Timeout"

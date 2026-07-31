"""Tests for wait_for_callback_timeout."""

from async_durable_execution import InvocationStatus
from examples.wait_for_callback import wait_for_callback_timeout


async def test_handle_wait_for_callback_timeout_scenarios(durable_runner) -> None:
    """Test waitForCallback timeout scenarios."""
    test_payload = {"test": "timeout-scenario"}

    async with durable_runner(
        handler=wait_for_callback_timeout.handler, input=test_payload, timeout=2
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
    assert result_data["error"] == "Callback timed out: Callback.Timeout"

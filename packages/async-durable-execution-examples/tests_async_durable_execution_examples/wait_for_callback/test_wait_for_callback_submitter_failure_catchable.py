"""Tests for wait_for_callback_failing_submitter."""

from async_durable_execution_examples.wait_for_callback import (
    wait_for_callback_submitter_failure_catchable,
)


def test_handle_wait_for_callback_with_failing_submitter_function_errors(
    durable_runner,
):
    """Test waitForCallback with failing submitter function errors."""
    with durable_runner(
        handler=wait_for_callback_submitter_failure_catchable.handler,
        input=None,
        timeout=30,
    ) as runner:
        execution_arn = runner.run_async()
        result = runner.wait_for_result(execution_arn=execution_arn)

    result_data = result.get_deserialized_result()

    assert result_data == {
        "success": False,
        "error": "Submitter failed",
    }

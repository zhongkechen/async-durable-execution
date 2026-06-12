"""Tests for wait_for_callback_submitter_retry_success."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.wait_for_callback import (
    wait_for_callback_submitter_failure,
)


def test_fail_after_exhausting_retries_when_submitter_always_fails(durable_runner):
    """Test that execution fails after exhausting retries when submitter always fails."""
    test_payload = {"shouldFail": True}

    with durable_runner(
        handler=wait_for_callback_submitter_failure.handler,
        input=test_payload,
        timeout=30,
    ) as runner:
        execution_arn = runner.run_async()
        result = runner.wait_for_result(execution_arn=execution_arn)

    # Execution should fail after retries are exhausted
    assert result.status is InvocationStatus.FAILED

    # Verify error details
    error = result.error
    assert error is not None
    assert "Simulated submitter failure" in error.message

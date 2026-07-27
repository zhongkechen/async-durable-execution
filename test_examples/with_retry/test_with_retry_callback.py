"""Tests for with_retry_callback example.

Demonstrates that with_retry retries the entire wait_for_callback flow
when the callback fails. The external system fails 2 times before
succeeding on the 3rd attempt.
"""

from async_durable_execution import InvocationStatus
from async_durable_execution import ErrorObject
from examples.with_retry import with_retry_callback


async def test_with_retry_callback_fails_twice_then_succeeds(durable_runner) -> None:
    """Test that with_retry retries the callback flow after failures.

    The external system sends callback failure 2 times, then succeeds
    on the 3rd attempt. with_retry handles the failures and retries
    the entire wait_for_callback block.
    """
    async with durable_runner(
        handler=with_retry_callback.handler, input=None, timeout=60
    ) as runner:
        execution_arn = await runner.run_async()

        # Attempt 1: external system fails
        callback_id_1 = await runner.wait_for_callback(
            execution_arn=execution_arn,
            name="external-call-attempt-1-callback",
        )
        await runner.send_callback_failure(
            callback_id=callback_id_1,
            error=ErrorObject.from_message("External system unavailable"),
        )

        # Attempt 2: external system fails again
        callback_id_2 = await runner.wait_for_callback(
            execution_arn=execution_arn,
            name="external-call-attempt-2-callback",
        )
        await runner.send_callback_failure(
            callback_id=callback_id_2,
            error=ErrorObject.from_message("External system timeout"),
        )

        # Attempt 3: external system succeeds
        callback_id_3 = await runner.wait_for_callback(
            execution_arn=execution_arn,
            name="external-call-attempt-3-callback",
        )
        await runner.send_callback_success(
            callback_id=callback_id_3,
            result=b"approval granted",
        )

        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()
    assert result_data == {
        "success": True,
        "result": "approval granted",
    }

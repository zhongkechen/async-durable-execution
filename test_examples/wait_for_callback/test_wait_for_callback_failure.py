from async_durable_execution import InvocationStatus
from async_durable_execution import ErrorObject
from examples.wait_for_callback import wait_for_callback


async def test_wait_for_callback_failure(durable_runner) -> None:
    async with durable_runner(
        handler=wait_for_callback.handler, input="test", timeout=30
    ) as runner:
        execution_arn = await runner.run_async()
        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)
        await runner.send_callback_failure(
            callback_id=callback_id, error=ErrorObject.from_message("my callback error")
        )
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.FAILED
    assert isinstance(result.error, ErrorObject)
    assert result.error.message == "my callback error"
    assert result.error.type == "CallbackError"

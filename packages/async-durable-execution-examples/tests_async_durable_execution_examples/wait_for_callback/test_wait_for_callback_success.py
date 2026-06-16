from async_durable_execution import InvocationStatus
from async_durable_execution_examples.wait_for_callback import wait_for_callback


async def test_wait_for_callback_success(durable_runner):
    with durable_runner(
        handler=wait_for_callback.handler, input="test", timeout=30
    ) as runner:
        execution_arn = await runner.run_async()
        callback_id = await runner.wait_for_callback(execution_arn=execution_arn)
        await runner.send_callback_success(
            callback_id=callback_id, result=b"callback success"
        )
        result = await runner.wait_for_result(execution_arn=execution_arn)
    assert result.status is InvocationStatus.SUCCEEDED
    assert (
        result.get_deserialized_result() == "External system result: callback success"
    )

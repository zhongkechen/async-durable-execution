from async_durable_execution.execution import InvocationStatus
from async_durable_execution.lambda_service import ErrorObject
from async_durable_execution_examples.wait_for_callback import wait_for_callback


def test_wait_for_callback_failure(durable_runner):
    with durable_runner(
        handler=wait_for_callback.handler, input="test", timeout=30
    ) as runner:
        execution_arn = runner.run_async()
        callback_id = runner.wait_for_callback(execution_arn=execution_arn)
        runner.send_callback_failure(
            callback_id=callback_id, error=ErrorObject.from_message("my callback error")
        )
        result = runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.FAILED
    assert isinstance(result.error, ErrorObject)
    assert result.error.to_dict() == {
        "ErrorMessage": "my callback error",
        "ErrorType": "CallableRuntimeError",
    }

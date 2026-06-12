"""Tests for callback example."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.callback import callback_simple


def test_callback_success(durable_runner):
    callback_result = "successful"

    with durable_runner(
        handler=callback_simple.handler, input=None, timeout=30
    ) as runner:
        execution_arn = runner.run_async()
        callback_id = runner.wait_for_callback(execution_arn=execution_arn)
        runner.send_callback_success(
            callback_id=callback_id, result=callback_result.encode()
        )
        result = runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()
    assert result_data == callback_result


def test_callback_success_none_result(durable_runner):
    with durable_runner(
        handler=callback_simple.handler, input=None, timeout=30
    ) as runner:
        execution_arn = runner.run_async()
        callback_id = runner.wait_for_callback(execution_arn=execution_arn)
        runner.send_callback_success(callback_id=callback_id, result=b"")
        result = runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()
    assert result_data is None

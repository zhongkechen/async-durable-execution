"""Tests for the cloud sync client example."""

from async_durable_execution import InvocationStatus
from async_durable_execution.client import lambda_api_client_is_async
from examples.cloud_sync_client import cloud_sync_client


def test_cloud_sync_client_example_configures_sync_lambda_client():
    client = cloud_sync_client.handler._durable_execution_boto3_client

    assert client is not None
    assert not lambda_api_client_is_async(client)


async def test_cloud_sync_client_example(durable_runner):
    async with durable_runner(
        handler=cloud_sync_client.handler,
        input={"name": "sync-cloud"},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "client": "sync",
        "message": "hello sync-cloud",
    }

    step_result = result.get_step("format-message")
    assert result.get_operation_deserialized_result(step_result) == "hello sync-cloud"

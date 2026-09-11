"""Tests for the cloud sync client example."""

import inspect
import pytest

from async_durable_execution import InvocationStatus


@pytest.fixture
def cloud_sync_client(request, monkeypatch):
    # Client construction happens at import time. Local tests must not consult
    # instance metadata or depend on the developer's AWS configuration.
    if request.config.getoption("--runner-mode") == "local":
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
    from examples.cloud_sync_client import cloud_sync_client

    return cloud_sync_client


def test_cloud_sync_client_example_configures_sync_lambda_client(
    cloud_sync_client,
) -> None:
    client = cloud_sync_client._sync_lambda_client

    assert client is not None
    assert not inspect.iscoroutinefunction(client.checkpoint_durable_execution)


async def test_cloud_sync_client_example(durable_runner, cloud_sync_client) -> None:
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

"""Integration tests for hello world example."""

from async_durable_execution import InvocationStatus
from examples.core import hello_world


async def test_hello_world(durable_runner):
    """Test hello world example."""
    async with durable_runner(
        handler=hello_world.handler, input="test", timeout=30
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "statusCode": 200,
        "body": "Hello from Durable Lambda! (status: 200)",
    }

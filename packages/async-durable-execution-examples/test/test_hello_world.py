"""Integration tests for hello world example."""

import pytest
from async_durable_execution.execution import InvocationStatus

from async_durable_execution_examples import hello_world
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=hello_world.handler,
)
def test_hello_world(durable_runner):
    """Test hello world example."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == {
        "statusCode": 200,
        "body": "Hello from Durable Lambda! (status: 200)",
    }

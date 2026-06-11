"""Tests for wait_for_condition."""

import pytest
from async_durable_execution.execution import InvocationStatus

from async_durable_execution_examples.wait_for_condition import wait_for_condition
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_condition.handler,
)
def test_wait_for_condition(durable_runner):
    """Test wait_for_condition pattern."""
    pass
    # TODO: fix bug in local runner so that local tests can pass
    # with durable_runner:
    #     result = durable_runner.run(input="test", timeout=30)

    # assert result.status is InvocationStatus.SUCCEEDED
    # # Should reach state 3 after 3 increments
    # assert deserialize_operation_payload(result.result) == 3

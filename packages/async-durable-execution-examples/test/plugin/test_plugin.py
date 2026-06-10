"""Tests for step example."""

import pytest
from async_durable_execution.execution import InvocationStatus

from async_durable_execution_examples.plugin import execution_with_plugin
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=execution_with_plugin.handler,
)
def test_plugin(durable_runner):
    """Test basic step example."""
    with durable_runner:
        result = durable_runner.run(input="{}", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == 12

    step_result = result.get_step("add-result-to-2")
    assert deserialize_operation_payload(step_result.result) == 12

"""Tests for step_with_class_methods example."""

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.step import step_with_class_methods


async def test_step_with_class_methods(durable_runner):
    """Test durable callable instance, class, and static methods."""
    with durable_runner(
        handler=step_with_class_methods.handler,
        input={"prices": [12.0, 8.0, 5.0], "tax_rate": 0.2},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "subtotal": 25.0,
        "discount": 2.5,
        "total": 27.0,
    }
    assert result.get_operation_deserialized_result(result.get_step("subtotal")) == 25.0
    assert result.get_operation_deserialized_result(result.get_step("discount")) == 2.5
    assert result.get_operation_deserialized_result(result.get_step("total")) == 27.0

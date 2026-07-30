"""Tests for synchronous durable step examples."""

from async_durable_execution import InvocationStatus
from examples.sync_functions import sync_steps


async def test_sync_steps(durable_runner):
    """Execute synchronous durable functions and bound methods."""
    async with durable_runner(
        handler=sync_steps.handler,
        input={"sku": "book", "quantity": 3, "tax_rate": 0.2},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "sku": "book",
        "quantity": 3,
        "unit_price": 12.5,
        "total": 45.0,
    }
    assert (
        result.get_operation_deserialized_result(result.get_step("load-unit-price"))
        == 12.5
    )
    assert (
        result.get_operation_deserialized_result(result.get_step("calculate-total"))
        == 45.0
    )

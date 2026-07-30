"""Tests for the synchronous handler example."""

from async_durable_execution import InvocationStatus
from examples.sync_functions import sync_handler


async def test_sync_handler(durable_runner):
    """Execute a synchronous durable handler."""
    async with durable_runner(
        handler=sync_handler.handler,
        input={"order_id": "order-456", "quantity": 3, "unit_price": 4.5},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "order_id": "order-456",
        "quantity": 3,
        "total": 13.5,
    }
    assert result.operations == []

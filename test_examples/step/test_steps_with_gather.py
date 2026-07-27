"""Tests for asyncio.gather step example."""

from async_durable_execution import InvocationStatus
from examples.step import steps_with_gather


async def test_steps_with_gather(durable_runner) -> None:
    """Test multiple step tasks awaited with asyncio.gather."""
    input_items = {
        "items": [
            {"sku": "notebook", "quantity": 3, "unit_price": 7},
            {"sku": "pen", "quantity": 10, "unit_price": 2},
            {"sku": "bag", "quantity": 1, "unit_price": 35},
        ]
    }

    async with durable_runner(
        handler=steps_with_gather.handler,
        input=input_items,
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "items": [
            {"sku": "notebook", "quantity": 3, "line_total": 21},
            {"sku": "pen", "quantity": 10, "line_total": 20},
            {"sku": "bag", "quantity": 1, "line_total": 35},
        ],
        "total": 76,
    }

    assert result.get_operation_deserialized_result(
        result.get_step("price-notebook")
    ) == {"sku": "notebook", "quantity": 3, "line_total": 21}
    assert result.get_operation_deserialized_result(result.get_step("price-pen")) == {
        "sku": "pen",
        "quantity": 10,
        "line_total": 20,
    }
    assert result.get_operation_deserialized_result(result.get_step("price-bag")) == {
        "sku": "bag",
        "quantity": 1,
        "line_total": 35,
    }

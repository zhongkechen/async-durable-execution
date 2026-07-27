"""Tests for map_with_item_namer example."""

from async_durable_execution import InvocationStatus
from async_durable_execution import (
    OperationStatus,
)
from examples.extension.map import map_with_item_namer


async def test_map_with_item_namer(durable_runner):
    """Test map example with custom item_namer for iteration naming."""
    async with durable_runner(
        handler=map_with_item_namer.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == [
        "processed-order-101-$25",
        "processed-order-102-$50",
        "processed-order-103-$75",
    ]

    # Get the map operation
    map_op = result.get_context("process_orders")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

    # Verify custom iteration names from item_namer
    assert len(result.get_child_operations(map_op)) == 3
    child_names = {op.name for op in result.get_child_operations(map_op)}
    expected_names = {"order-order-101", "order-order-102", "order-order-103"}
    assert child_names == expected_names

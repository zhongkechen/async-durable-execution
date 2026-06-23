"""Tests for invoke example."""

import pytest

from async_durable_execution import InvocationStatus, OperationStatus
from async_durable_execution_examples.invoke import invoke


async def test_invoke_uses_mocked_child_result(durable_runner):
    child_function_name = "price-order-child:$LATEST"
    child_result = {"price": 42, "currency": "USD"}

    with durable_runner(
        handler=invoke.handler,
        input={
            "order_id": "order-123",
            "child_function_name": child_function_name,
            "tenant_id": "tenant-abc",
        },
        timeout=10,
    ) as runner:
        if runner.mode == "cloud":
            pytest.skip("invoke example uses a local runner mock child result")
        runner.mock_invoke_result(child_function_name, child_result)
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "orderId": "order-123",
        "price": 42,
        "currency": "USD",
    }

    invoke_operation = result.get_invoke("price-order")
    assert invoke_operation.status is OperationStatus.SUCCEEDED
    assert invoke_operation.get_deserialized_result() == child_result

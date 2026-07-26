"""Tests for invoke example."""

import os

import pytest

from async_durable_execution import InvocationStatus, OperationStatus
from examples.function_naming import to_function_name_suffix
from examples.invoke import invoke


CHILD_HANDLER = "examples.invoke.price_order_child.handler"


async def test_invoke_uses_mocked_child_result(durable_runner, request):
    child_result = {"price": 42, "currency": "USD"}
    runner_mode = request.config.getoption("--runner-mode")
    child_function_name = _get_child_function_name(runner_mode)
    input_payload = {
        "order_id": "order-123",
        "child_function_name": child_function_name,
    }
    if runner_mode != "cloud":
        input_payload["tenant_id"] = "tenant-abc"

    async with durable_runner(
        handler=invoke.handler,
        input=input_payload,
        timeout=10,
    ) as runner:
        if runner.mode != "cloud":
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
    assert result.get_operation_deserialized_result(invoke_operation) == child_result


def _get_child_function_name(runner_mode: str) -> str:
    if runner_mode != "cloud":
        return "price-order-child:$LATEST"

    function_name_prefix = os.environ.get("PYTEST_FUNCTION_NAME_PREFIX")
    if function_name_prefix:
        child_name = f"{function_name_prefix}{to_function_name_suffix(CHILD_HANDLER)}"
        return f"{child_name}:$LATEST"

    pytest.fail(
        "Cloud invoke test requires PYTEST_FUNCTION_NAME_PREFIX so the deployed "
        "child function name can be derived."
    )

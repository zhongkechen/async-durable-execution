"""Tests for recurse example."""

import pytest

from async_durable_execution import InvocationStatus, OperationStatus
from async_durable_execution_examples.invoke import recurse


LOCAL_RECURSE_FUNCTION_NAME = (
    "arn:aws:lambda:us-west-2:123456789012:function:test-function"
)
EXPECTED_RECURSION_ERROR_TYPES = {
    "CallableRuntimeError",
    "RecursiveInvocationException",
}


def _middle_pivot_chain_values(count: int, start: int = 1) -> list[int]:
    if count <= 0:
        return []
    if count == 1:
        return [start]

    remaining_values = [start, *_middle_pivot_chain_values(count - 2, start + 2)]
    pivot_index = count // 2
    return [
        *remaining_values[:pivot_index],
        start + 1,
        *remaining_values[pivot_index:],
    ]


THIRTY_ONE_VALUES = _middle_pivot_chain_values(31)
THIRTY_THREE_VALUES = _middle_pivot_chain_values(33)


async def test_recurse_base_case_returns_current_recursive_level(durable_runner):
    async with durable_runner(
        handler=recurse.handler,
        input={"values": [7]},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "sorted": [7],
        "count": 1,
        "recursive_level": 0,
    }


async def test_recurse_31_values_does_not_trigger_lambda_recursion_protection(
    durable_runner,
    request,
):
    runner_mode = request.config.getoption("--runner-mode")

    async with durable_runner(
        handler=recurse.handler,
        # Lambda counts the original invocation too, so SDK recursive level 14
        # is 15 total Lambda invocations.
        input={"values": THIRTY_ONE_VALUES},
        timeout=120,
    ) as runner:
        if runner_mode != "cloud":
            runner.mock_invoke_result(
                LOCAL_RECURSE_FUNCTION_NAME,
                {
                    "sorted": sorted(value for value in THIRTY_ONE_VALUES if value > 2),
                    "count": len(THIRTY_ONE_VALUES) - 2,
                    "recursive_level": 14,
                },
            )
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "sorted": sorted(THIRTY_ONE_VALUES),
        "count": len(THIRTY_ONE_VALUES),
        "recursive_level": 14,
    }

    first_invoke = result.get_invoke("sort-right-1")
    assert first_invoke.status is OperationStatus.SUCCEEDED
    assert result.get_operation_deserialized_result(first_invoke) == {
        "sorted": sorted(value for value in THIRTY_ONE_VALUES if value > 2),
        "count": len(THIRTY_ONE_VALUES) - 2,
        "recursive_level": 14,
    }


async def test_recurse_33_values_triggers_lambda_recursion_protection(
    durable_runner,
    request,
):
    if request.config.getoption("--runner-mode") != "cloud":
        pytest.skip("Lambda recursion protection is only enforced in cloud mode")

    async with durable_runner(
        handler=recurse.handler,
        # SDK recursive level 15 is 16 total Lambda invocations, which Lambda
        # rejects with recursion protection.
        input={"values": THIRTY_THREE_VALUES},
        timeout=120,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert result.error.type in EXPECTED_RECURSION_ERROR_TYPES
    assert "recursion" in result.error.message.lower()

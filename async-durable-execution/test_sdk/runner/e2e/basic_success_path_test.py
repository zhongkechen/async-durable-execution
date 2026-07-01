"""Functional tests, covering end-to-end DurableTestRunner."""

import json
from datetime import timedelta
from functools import partial
from typing import Any

from async_durable_execution import (
    DurableFunctionLocalTestRunner,
    DurableFunctionTestResult,
    InvocationStatus,
    durable_execution,
    run_in_child_context,
)
from async_durable_execution import step, wait


# brazil-test-exec pytest test/runner_int_test.py
async def test_basic_durable_function() -> None:
    async def one(a: int, b: int) -> str:
        # print("[DEBUG] one called")
        return f"{a} {b}"

    async def two_1(a: int, b: int) -> str:
        # print("[DEBUG] two_1 called")
        return f"{a} {b}"

    async def two_2(a: int, b: int) -> str:
        # print("[DEBUG] two_2 called")
        return f"{b} {a}"

    async def two(a: int, b: int) -> str:
        # print("[DEBUG] two called")
        two_1_result: str = await step(partial(two_1, a, b))
        two_2_result: str = await step(partial(two_2, a, b))
        return f"{two_1_result} {two_2_result}"

    async def three(a: int, b: int) -> str:
        # print("[DEBUG] three called")
        return f"{a} {b}"

    @durable_execution
    async def function_under_test(event: Any) -> list[str]:
        results: list[str] = []

        result_one: str = await step(partial(one, 1, 2), name="one")
        results.append(result_one)

        await wait(timedelta(seconds=1))

        result_two: str = await run_in_child_context(
            partial(two, 3, 4),
            name="two",
        )
        results.append(result_two)

        result_three: str = await step(partial(three, 5, 6), name="three")
        results.append(result_three)

        return results

    with DurableFunctionLocalTestRunner(
        handler=function_under_test, input="input str", timeout=10
    ) as runner:
        result: DurableFunctionTestResult = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps(["1 2", "3 4 4 3", "5 6"])

    one_result = result.get_step("one")
    assert one_result.step_details is not None
    assert one_result.step_details.result == json.dumps("1 2")

    two_result = result.get_context("two")
    assert two_result.context_details is not None
    assert two_result.context_details.result == json.dumps("3 4 4 3")

    three_result = result.get_step("three")
    assert three_result.step_details is not None
    assert three_result.step_details.result == json.dumps("5 6")

    # currently has the optimization where it's not saving child checkpoints after parent done
    # prob should unpick that for test
    # two_one_op = next(op for op in result.get_child_operations(two_result) if op.name == "two_1")
    # assert two_one_op.step_details.result == '"3 4"'

    # print("done")

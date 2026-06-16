"""Functional tests, covering end-to-end DurableTestRunner."""

import asyncio
import json
from datetime import timedelta
from functools import partial
from typing import Any, cast

from async_durable_execution.context import DurableContext, get_context
from async_durable_execution.execution import (
    InvocationStatus,
    durable_execution,
)
from async_durable_execution_runner.runner import (
    ContextOperation,
    DurableFunctionLocalTestRunner,
    DurableFunctionTestResult,
    StepOperation,
)


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
        ctx = cast(DurableContext, get_context())
        two_1_result: str = await ctx.step(partial(two_1, a, b))
        two_2_result: str = await ctx.step(partial(two_2, a, b))
        return f"{two_1_result} {two_2_result}"

    async def three(a: int, b: int) -> str:
        # print("[DEBUG] three called")
        return f"{a} {b}"

    @durable_execution
    async def function_under_test(event: Any) -> list[str]:
        context = cast(DurableContext, get_context())
        results: list[str] = []

        result_one: str = await context.step(partial(one, 1, 2))
        results.append(result_one)

        await context.wait(timedelta(seconds=1))

        result_two: str = await context.run_in_child_context(
            partial(two, 3, 4),
            name="two",
        )
        results.append(result_two)

        result_three: str = await context.step(partial(three, 5, 6))
        results.append(result_three)

        return results

    with DurableFunctionLocalTestRunner(
        handler=function_under_test, input="input str", timeout=10
    ) as runner:
        result: DurableFunctionTestResult = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps(["1 2", "3 4 4 3", "5 6"])

    one_result: StepOperation = result.get_step("one")
    assert one_result.result == json.dumps("1 2")

    two_result: ContextOperation = result.get_context("two")
    assert two_result.result == json.dumps("3 4 4 3")

    three_result: StepOperation = result.get_step("three")
    assert three_result.result == json.dumps("5 6")

    # currently has the optimization where it's not saving child checkpoints after parent done
    # prob should unpick that for test
    # two_one_op = cast(StepOperation, two_result_op.get_operation_by_name("two_1"))
    # assert two_one_op.result == '"3 4"'

    # print("done")

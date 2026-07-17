"""6-12: Wait-for-condition followed by a step."""

from typing import Any

from async_durable_execution import (
    WaitForConditionDecision,
    durable_callable,
    durable_execution,
    step,
    wait_for_condition,
)


@durable_callable
async def multiply(value: int) -> int:
    return value * 10


@durable_execution
async def handler(event: Any) -> int:
    threshold = int(event)

    async def check(state: int | None):
        next_state = (state or 0) + 1
        decision = (
            WaitForConditionDecision.stop_polling()
            if next_state >= threshold
            else WaitForConditionDecision.continue_waiting()
        )
        return next_state, decision

    result = await wait_for_condition(
        check, initial_state=0, wait_strategy=lambda _s, _a: 1
    )
    return await step(multiply(result))

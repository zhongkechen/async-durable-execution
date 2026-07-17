"""6-1: Wait-for-condition basic."""

from typing import Any

from async_durable_execution import (
    WaitForConditionDecision,
    durable_execution,
    wait_for_condition,
)


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

    return await wait_for_condition(
        check, initial_state=0, wait_strategy=lambda _s, _a: 1
    )

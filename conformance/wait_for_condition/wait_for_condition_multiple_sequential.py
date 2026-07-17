"""6-13: Multiple sequential wait-for-condition operations."""

from typing import Any

from async_durable_execution import (
    WaitForConditionDecision,
    durable_execution,
    wait_for_condition,
)


def make_check(threshold: int):
    async def check(state: int | None):
        next_state = (state or 0) + 1
        decision = (
            WaitForConditionDecision.stop_polling()
            if next_state >= threshold
            else WaitForConditionDecision.continue_waiting()
        )
        return next_state, decision

    return check


@durable_execution
async def handler(_event: Any) -> int:
    first = await wait_for_condition(
        make_check(2), initial_state=0, wait_strategy=lambda _s, _a: 1
    )
    return await wait_for_condition(
        make_check(4), initial_state=first, wait_strategy=lambda _s, _a: 1
    )

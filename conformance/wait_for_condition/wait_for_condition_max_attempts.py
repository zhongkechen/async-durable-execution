"""6-6: Wait-for-condition max attempts exceeded."""

from typing import Any

from async_durable_execution import (
    WaitForConditionDecision,
    durable_execution,
    get_attempt,
    wait_for_condition,
)


@durable_execution
async def handler(_event: Any) -> int:
    async def check(state: int | None):
        if (get_attempt() or 0) >= 3:
            raise RuntimeError("maximum polling attempts exceeded")
        return (state or 0) + 1, WaitForConditionDecision.continue_waiting()

    return await wait_for_condition(
        check, initial_state=0, wait_strategy=lambda _s, _a: 1
    )

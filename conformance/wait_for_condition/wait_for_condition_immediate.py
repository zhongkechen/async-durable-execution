"""6-2: Wait-for-condition immediate stop."""

from typing import Any

from async_durable_execution import (
    WaitForConditionDecision,
    durable_execution,
    wait_for_condition,
)


@durable_execution
async def handler(event: Any) -> int:
    async def check(state: int | None):
        value = int(state or 0)
        decision = (
            WaitForConditionDecision.stop_polling()
            if value >= 5
            else WaitForConditionDecision.continue_waiting()
        )
        return value, decision

    return await wait_for_condition(
        check, initial_state=int(event), wait_strategy=lambda _s, _a: 1
    )

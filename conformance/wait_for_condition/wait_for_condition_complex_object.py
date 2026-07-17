"""6-9: Wait-for-condition with structured state."""

from typing import Any

from async_durable_execution import (
    WaitForConditionDecision,
    durable_execution,
    wait_for_condition,
)


@durable_execution
async def handler(_event: Any) -> dict:
    async def check(state: dict | None):
        current = state or {"status": "PENDING", "attempts": 0}
        attempts = current["attempts"] + 1
        next_state = {
            "status": "DONE" if attempts >= 2 else "PENDING",
            "attempts": attempts,
        }
        decision = (
            WaitForConditionDecision.stop_polling()
            if next_state["status"] == "DONE"
            else WaitForConditionDecision.continue_waiting()
        )
        return next_state, decision

    return await wait_for_condition(
        check,
        initial_state={"status": "PENDING", "attempts": 0},
        wait_strategy=lambda _s, _a: 1,
    )

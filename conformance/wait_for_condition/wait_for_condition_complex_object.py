"""6-9: Wait-for-condition with structured state."""

from typing import Any

from async_durable_execution import durable_execution, wait_for_condition


@durable_execution
async def handler(_event: Any) -> dict:
    async def check(state: dict | None) -> dict:
        current = state or {"status": "PENDING", "attempts": 0}
        attempts = current["attempts"] + 1
        next_state = {
            "status": "DONE" if attempts >= 2 else "PENDING",
            "attempts": attempts,
        }
        return next_state

    return await wait_for_condition(
        check,
        initial_state={"status": "PENDING", "attempts": 0},
        polling_strategy=lambda state, _attempt: (
            None if state["status"] == "DONE" else 1
        ),
    )

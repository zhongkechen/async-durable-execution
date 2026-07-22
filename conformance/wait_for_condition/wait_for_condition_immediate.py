"""6-2: Wait-for-condition immediate stop."""

from typing import Any

from async_durable_execution import durable_execution, wait_for_condition


@durable_execution
async def handler(event: Any) -> int:
    async def check(state: int | None):
        return int(state or 0)

    return await wait_for_condition(
        check,
        initial_state=int(event),
        polling_strategy=lambda state, _attempt: None if state >= 5 else 1,
    )

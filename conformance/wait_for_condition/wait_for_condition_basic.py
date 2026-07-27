"""6-1: Wait-for-condition basic."""

from typing import Any

from async_durable_execution import durable_execution, wait_for_condition


@durable_execution
async def handler(event: Any) -> int:
    threshold = int(event)

    async def check(state: int | None) -> int:
        return (state or 0) + 1

    return await wait_for_condition(
        check,
        initial_state=0,
        polling_strategy=lambda state, _attempt: (None if state >= threshold else 1),
    )

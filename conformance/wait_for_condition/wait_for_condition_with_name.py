"""6-3: Wait-for-condition with an explicit name."""

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
        name="poll-status",
        polling_strategy=lambda state, _attempt: (None if state >= threshold else 1),
    )

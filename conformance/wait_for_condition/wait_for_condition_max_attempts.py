"""6-6: Wait-for-condition max attempts exceeded."""

from typing import Any

from async_durable_execution import (
    durable_execution,
    wait_for_condition,
)


@durable_execution
async def handler(_event: Any) -> int:
    async def check(state: int | None):
        return (state or 0) + 1

    def polling_strategy(_state: int, attempt: int) -> int:
        if attempt >= 3:
            raise RuntimeError("maximum polling attempts exceeded")
        return 1

    return await wait_for_condition(
        check,
        initial_state=0,
        polling_strategy=polling_strategy,
    )

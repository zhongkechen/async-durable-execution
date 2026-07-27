"""6-10: Wait-for-condition null result."""

from typing import Any

from async_durable_execution import durable_execution, wait_for_condition


@durable_execution
async def handler(_event: Any) -> None:
    async def check(_state: None) -> None:
        return None

    return await wait_for_condition(
        check,
        initial_state=None,
        polling_strategy=lambda _state, _attempt: None,
    )

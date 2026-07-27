"""6-7: Wait-for-condition check throws."""

from typing import Any, NoReturn

from async_durable_execution import durable_execution, wait_for_condition


@durable_execution
async def handler(_event: Any) -> None:
    async def check(_state: None) -> NoReturn:
        raise RuntimeError("check function failed")

    return await wait_for_condition(check, initial_state=None)

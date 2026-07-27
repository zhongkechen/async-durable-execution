"""6-8: Wait-for-condition check failure caught by the handler."""

from typing import Any, NoReturn

from async_durable_execution import durable_execution, wait_for_condition


@durable_execution
async def handler(_event: Any) -> str:
    async def check(_state: None) -> NoReturn:
        raise RuntimeError("check function failed")

    try:
        await wait_for_condition(check, initial_state=None)
    except Exception:
        return "recovered"

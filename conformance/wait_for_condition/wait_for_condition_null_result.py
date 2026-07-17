"""6-10: Wait-for-condition null result."""

from typing import Any

from async_durable_execution import (
    WaitForConditionDecision,
    durable_execution,
    wait_for_condition,
)


@durable_execution
async def handler(_event: Any) -> None:
    async def check(_state: None):
        return None, WaitForConditionDecision.stop_polling()

    return await wait_for_condition(check, initial_state=None)

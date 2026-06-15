"""Example demonstrating wait-for-condition pattern."""

import asyncio
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_execution,
    WaitForConditionConfig,
    WaitForConditionDecision,
    wait_for_condition,
)


@durable_execution
async def handler(_event: Any) -> int:
    """Handler demonstrating wait-for-condition pattern."""

    async def condition_function(state: int, _) -> int:
        """Increment state by 1."""
        await asyncio.sleep(0)
        return state + 1

    def wait_strategy(state: int, attempt: int) -> dict[str, Any]:
        """Wait strategy that continues until state reaches 3."""
        if state >= 3:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(timedelta(seconds=1))

    config = WaitForConditionConfig(wait_strategy=wait_strategy, initial_state=0)

    result = await wait_for_condition(check=condition_function, config=config)

    return result

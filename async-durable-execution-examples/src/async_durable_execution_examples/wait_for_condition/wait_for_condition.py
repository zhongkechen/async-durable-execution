"""Example demonstrating wait-for-condition pattern."""

import asyncio
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_execution,
    get_current_context,
    WaitForConditionCheckContext,
    WaitForConditionConfig,
    WaitForConditionDecision,
    WaitStrategyBuilder,
    wait_for_condition,
)
from async_durable_execution.config import JitterStrategy


@durable_execution
async def handler(_event: Any) -> int:
    """Handler demonstrating wait-for-condition pattern."""

    async def check_function(
        state: int,
    ) -> tuple[int, WaitForConditionDecision]:
        """Increment state by 1."""
        await asyncio.sleep(0)
        assert isinstance(get_current_context(), WaitForConditionCheckContext)
        new_state = state + 1
        if new_state >= 3:
            return new_state, WaitForConditionDecision.stop_polling()
        return new_state, WaitForConditionDecision.continue_waiting()

    config = WaitForConditionConfig(
        wait_strategy=WaitStrategyBuilder[int](
            initial_delay=timedelta(seconds=1),
            jitter_strategy=JitterStrategy.NONE,
        ).build(),
    )

    result = await wait_for_condition(
        check=check_function,
        config=config,
        initial_state=0,
    )

    return result

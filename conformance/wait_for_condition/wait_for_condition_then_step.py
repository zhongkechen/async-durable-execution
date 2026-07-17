"""6-12: Wait-for-condition followed by a step."""

from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    step,
    wait_for_condition,
)


@durable_callable
async def multiply(value: int) -> int:
    return value * 10


@durable_execution
async def handler(event: Any) -> int:
    threshold = int(event)

    async def check(state: int | None):
        return (state or 0) + 1

    result = await wait_for_condition(
        check,
        initial_state=0,
        polling_strategy=lambda state, _attempt: (None if state >= threshold else 1),
    )
    return await step(multiply(result))

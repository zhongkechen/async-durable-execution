from typing import Any

from async_durable_execution import (
    durable_callable,
    step,
    StepSemantics,
    durable_execution,
)


@durable_execution
async def handler(_event: Any) -> str:
    @durable_callable
    async def at_most_once_step() -> str:
        return "AT_MOST_ONCE_PER_RETRY semantics"

    result = await step(
        at_most_once_step(),
        name="at_most_once_step",
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )
    return f"Result: {result}"

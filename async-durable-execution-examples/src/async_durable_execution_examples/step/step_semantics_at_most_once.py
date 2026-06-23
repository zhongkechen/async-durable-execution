from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_callable,
    step,
    StepConfig,
    StepSemantics,
    durable_execution,
)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> str:
    # Step with AT_MOST_ONCE_PER_RETRY semantics
    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)

    @durable_callable
    async def at_most_once_step() -> str:
        return "AT_MOST_ONCE_PER_RETRY semantics"

    result = await step(
        at_most_once_step(),
        name="at_most_once_step",
        config=config,
    )
    return f"Result: {result}"

from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_callable,
    step,
    durable_execution,
)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> str:
    # Step without explicit name - should use function name
    @durable_callable
    async def unnamed_step() -> str:
        return "Step without name"

    result = await step(unnamed_step())
    return f"Result: {result}"

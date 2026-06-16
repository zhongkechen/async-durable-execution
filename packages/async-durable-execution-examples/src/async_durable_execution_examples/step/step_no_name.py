from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    durable_execution,
)


@durable_execution
async def handler(_event: Any) -> str:
    # Step without explicit name - should use function name
    @durable_step
    async def unnamed_step() -> str:
        return "Step without name"

    result = await step(unnamed_step())
    return f"Result: {result}"

from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    durable_execution,
)


@durable_execution
async def handler(_event: Any) -> str:
    # Step name comes from the decorated function name
    @durable_step
    async def named_step() -> str:
        return "Step with explicit name"

    result = await step(named_step(), name="custom_step")
    return f"Result: {result}"

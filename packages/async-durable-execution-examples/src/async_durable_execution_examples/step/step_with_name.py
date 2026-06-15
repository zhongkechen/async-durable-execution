from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    # Step with explicit name
    async def named_step() -> str:
        return "Step with explicit name"

    result = await context.step(named_step, name="custom_step")
    return f"Result: {result}"

from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    # Step without explicit name - should use function name
    async def unnamed_step(_) -> str:
        return "Step without name"

    result = context.step(unnamed_step)
    return f"Result: {result}"

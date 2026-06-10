from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    # Step without explicit name - should use function name
    async def unnamed_step(_) -> str:
        return "Step without name"

    result = context.step(unnamed_step)
    return f"Result: {result}"

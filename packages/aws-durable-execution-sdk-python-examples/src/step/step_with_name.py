from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    # Step with explicit name
    async def named_step(_) -> str:
        return "Step with explicit name"

    result = context.step(named_step, name="custom_step")
    return f"Result: {result}"

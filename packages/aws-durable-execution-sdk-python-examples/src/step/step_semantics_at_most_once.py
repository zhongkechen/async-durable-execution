from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig, StepSemantics
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    # Step with AT_MOST_ONCE_PER_RETRY semantics
    config = StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY)

    async def at_most_once_step(_) -> str:
        return "AT_MOST_ONCE_PER_RETRY semantics"

    result = context.step(at_most_once_step, name="at_most_once_step", config=config)
    return f"Result: {result}"

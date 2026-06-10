from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution
from async_durable_execution.config import Duration


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(5))
    return "Wait completed"

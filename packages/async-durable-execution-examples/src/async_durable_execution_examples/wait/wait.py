from datetime import timedelta
from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    context.wait(timedelta(seconds=5))
    return "Wait completed"

"""Example demonstrating multiple sequential wait operations."""

from datetime import timedelta

from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating multiple sequential wait operations."""
    context.wait(timedelta(seconds=5), name="wait-1")
    context.wait(timedelta(seconds=5), name="wait-2")

    return {
        "completedWaits": 2,
        "finalStep": "done",
    }

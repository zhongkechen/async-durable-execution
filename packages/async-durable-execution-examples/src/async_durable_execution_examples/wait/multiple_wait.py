"""Example demonstrating multiple sequential wait operations."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_execution,
    wait,
)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> dict[str, Any]:
    """Handler demonstrating multiple sequential wait operations."""
    await wait(timedelta(seconds=1), name="wait-1")
    await wait(timedelta(seconds=1), name="wait-2")

    return {
        "completedWaits": 2,
        "finalStep": "done",
    }

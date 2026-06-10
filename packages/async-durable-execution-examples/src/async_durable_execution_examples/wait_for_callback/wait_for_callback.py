import asyncio
from typing import Any

from async_durable_execution.config import Duration, WaitForCallbackConfig
from async_durable_execution.context import (
    DurableContext,
    WaitForCallbackContext,
)
from async_durable_execution.execution import durable_execution


async def external_system_call(
    _callback_id: str, _context: WaitForCallbackContext
) -> None:
    """Simulate calling an external system with callback ID."""
    await asyncio.sleep(0)
    # In real usage, this would make an API call to an external system
    # passing the callback_id for the system to call back when done


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    config = WaitForCallbackConfig(
        timeout=Duration.from_seconds(120), heartbeat_timeout=Duration.from_seconds(60)
    )

    result = context.wait_for_callback(
        external_system_call, name="external_call", config=config
    )

    return f"External system result: {result}"

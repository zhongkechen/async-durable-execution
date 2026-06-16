import asyncio
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    WaitForCallbackConfig,
    WaitForCallbackContext,
    durable_execution,
    wait_for_callback,
)


async def external_system_call(
    _callback_id: str, _context: WaitForCallbackContext
) -> None:
    """Simulate calling an external system with callback ID."""
    await asyncio.sleep(0)
    # In real usage, this would make an API call to an external system
    # passing the callback_id for the system to call back when done


@durable_execution
async def handler(_event: Any) -> str:
    config = WaitForCallbackConfig(
        timeout=timedelta(seconds=120), heartbeat_timeout=timedelta(seconds=60)
    )

    result = await wait_for_callback(
        external_system_call, name="external_call", config=config
    )

    return f"External system result: {result}"

import asyncio
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    get_wait_for_callback_context,
    wait_for_callback,
)


@durable_callable
async def external_system_call() -> None:
    """Simulate calling an external system with callback ID."""
    await asyncio.sleep(0)
    callback_context = get_wait_for_callback_context()
    assert callback_context.callback_id
    # In real usage, this would make an API call to an external system
    # passing the callback_id for the system to call back when done


@durable_execution
async def handler(_event: Any) -> str:
    result = await wait_for_callback(
        external_system_call(),
        name="external_call",
        timeout=timedelta(seconds=120),
        heartbeat_timeout=timedelta(seconds=60),
    )

    return f"External system result: {result}"

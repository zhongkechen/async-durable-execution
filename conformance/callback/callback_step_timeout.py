from datetime import timedelta
from async_durable_execution import (
    create_callback,
    durable_callable,
    durable_execution,
    step,
)

# 4-8: Callback + Step + timeout
from typing import Any


@durable_callable
async def notify_external() -> str:
    return "notified"


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(name=event, timeout=timedelta(seconds=5))
    await step(notify_external(), name="notify-external")
    return await callback.result()

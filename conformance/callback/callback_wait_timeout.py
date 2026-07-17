from datetime import timedelta
from async_durable_execution import create_callback, durable_execution, wait

# 4-11: Callback + Wait + timeout (callback timeout < wait duration)
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(name=event, timeout=timedelta(seconds=3))
    await wait(timedelta(seconds=6), name="delay")
    return await callback.result()

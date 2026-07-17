from datetime import timedelta
from async_durable_execution import create_callback, durable_execution, wait

# 4-9: Callback + Wait + success
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(name=event)
    await wait(timedelta(seconds=5), name="delay")
    return await callback.result()

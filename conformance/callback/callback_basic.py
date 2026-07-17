from async_durable_execution import create_callback, durable_execution

# 4-1: Create callback basic (success via external callback)
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(name=event)
    return await callback.result()

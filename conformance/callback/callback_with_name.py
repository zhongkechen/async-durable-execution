from async_durable_execution import create_callback, durable_execution

# 4-2: Create callback with explicit name
from typing import Any


@durable_execution
async def handler(_event: Any) -> str:
    callback = await create_callback(name="approval")
    return await callback.result()

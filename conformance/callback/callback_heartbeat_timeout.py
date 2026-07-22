from datetime import timedelta
from async_durable_execution import create_callback, durable_execution

# 4-4: Create callback heartbeat timeout (no heartbeat sent)
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(name=event, heartbeat_timeout=timedelta(seconds=5))
    return await callback.result()

from datetime import timedelta
from async_durable_execution import create_callback, durable_execution

# 4-5: Create callback with heartbeat then success
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(
        name=event, heartbeat_timeout=timedelta(seconds=10)
    )
    return await callback.result()

from datetime import timedelta
from async_durable_execution import create_callback, durable_execution

# 4-3: Create callback general timeout (no external callback sent)
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(name=event, timeout=timedelta(seconds=5))
    return await callback.result()

from async_durable_execution import create_callback, durable_execution

# 4-6: Callback failure (external system reports failure)
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(name=event)
    # Do not catch — let the exception propagate so the execution fails.
    return await callback.result()

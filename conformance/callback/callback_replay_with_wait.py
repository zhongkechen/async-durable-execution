from datetime import timedelta
from async_durable_execution import create_callback, durable_execution, wait

# 4-12: Callback success → Wait → verify replay
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(name=event)
    cb_result = await callback.result()
    await wait(timedelta(seconds=2), name="after-cb")
    return cb_result

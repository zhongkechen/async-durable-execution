from datetime import timedelta
from async_durable_execution import (
    CallbackError,
    create_callback,
    durable_execution,
    wait,
)

# 4-14: Callback timeout caught → Wait → return
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    callback = await create_callback(name=event, timeout=timedelta(seconds=3))

    try:
        outcome = await callback.result()
    except CallbackError as e:
        outcome = f"caught_timeout:{e}"
    except Exception as e:  # pragma: no cover - safety net
        outcome = f"caught_other:{type(e).__name__}:{e}"

    await wait(timedelta(seconds=2), name="after-cb")
    return outcome

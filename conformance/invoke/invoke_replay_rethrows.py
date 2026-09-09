"""5-10: Invoke replay re-throws (failed invoke error re-thrown from cache)."""

from datetime import timedelta
from async_durable_execution import durable_execution, invoke, wait
import os
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    try:
        await invoke(function_name, event)
    except Exception:
        pass
    await wait(timedelta(seconds=1))
    return "caught and continued"

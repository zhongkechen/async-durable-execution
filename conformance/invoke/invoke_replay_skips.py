"""5-9: Invoke replay skips (invoke result cached on replay)."""

from datetime import timedelta
from async_durable_execution import durable_execution, invoke, wait
import os
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    result: str = await invoke(function_name, event)
    await wait(timedelta(seconds=1))
    return result

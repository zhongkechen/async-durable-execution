"""5-18: Invoke timeout, caught (timeout caught, execution continues)."""

from async_durable_execution import durable_execution, invoke
import os
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    try:
        await invoke(function_name, event)
    except Exception:
        pass
    return "timeout fallback"

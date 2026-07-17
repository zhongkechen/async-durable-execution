"""5-17: Invoke timeout (target takes too long)."""

from async_durable_execution import durable_execution, invoke
import os
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    result: str = await invoke(function_name, event)
    return result

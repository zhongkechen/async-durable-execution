"""5-2: Invoke with name (explicit name parameter from input)."""

from async_durable_execution import durable_execution, invoke
import os
from typing import Any


@durable_execution
async def handler(event: Any) -> Any:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    return await invoke(function_name, event["payload"], name=event["name"])

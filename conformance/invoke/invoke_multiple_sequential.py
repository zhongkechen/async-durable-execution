"""5-14: Multiple sequential invokes."""

from async_durable_execution import durable_execution, invoke
import os
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    function_name_1 = os.environ["TARGET_FUNCTION_NAME_1"]
    function_name_2 = os.environ["TARGET_FUNCTION_NAME_2"]
    result1: str = await invoke(function_name_1, event)
    result2: str = await invoke(function_name_2, result1)
    return result2

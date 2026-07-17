"""5-7: Invoke large payload (payload near size limit)."""

from async_durable_execution import durable_execution, invoke
import os
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    large_payload = {"data": "x" * 200000}
    result: str = await invoke(function_name, large_payload)
    return result

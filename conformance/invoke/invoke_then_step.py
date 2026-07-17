"""5-12: Invoke then step (invoke result used by subsequent step)."""

from async_durable_execution import durable_callable, durable_execution, invoke, step
import os
from typing import Any


@durable_callable
async def process_result(value: str) -> str:
    return f"processed: {value}"


@durable_execution
async def handler(event: Any) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    invoke_result: str = await invoke(function_name, event)
    result: str = await step(process_result(invoke_result))
    return result

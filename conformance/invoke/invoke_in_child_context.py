"""5-13: Invoke inside child context."""

from async_durable_execution import (
    durable_callable,
    durable_execution,
    invoke,
    run_in_child_context,
)
import os
from typing import Any


@durable_callable
async def invoke_in_child() -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    return await invoke(function_name, None)


@durable_execution
async def handler(event: Any) -> str:
    result: str = await run_in_child_context(invoke_in_child())
    return result

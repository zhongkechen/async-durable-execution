"""5-11: Step then invoke (sequential operations)."""

from async_durable_execution import durable_callable, durable_execution, invoke, step
import os
from typing import Any


@durable_callable
async def compute_payload() -> str:
    return "step result"


@durable_execution
async def handler(event: Any) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    step_result: str = await step(compute_payload())
    result: str = await invoke(function_name, step_result)
    return result

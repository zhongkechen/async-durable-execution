"""3-17: Child context with print only (verify no re-execution on replay)."""

from datetime import timedelta
from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    wait,
)
from typing import Any


@durable_callable
async def print_child(*, input_1: str) -> str:
    print(input_1, flush=True)
    return input_1


@durable_execution
async def handler(event: Any) -> str:
    result: str = await run_in_child_context(
        print_child(input_1=str(event)), name="print-child"
    )
    await wait(timedelta(seconds=1))
    return result

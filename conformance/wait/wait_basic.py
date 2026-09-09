"""2-1: Wait basic."""

from datetime import timedelta
from async_durable_execution import durable_execution, wait
from typing import Any


@durable_execution
async def handler(_event: Any) -> str:
    await wait(timedelta(seconds=2))
    return "Wait completed"

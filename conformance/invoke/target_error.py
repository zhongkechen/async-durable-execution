"""Target function that waits briefly then raises an exception."""

from datetime import timedelta
from async_durable_execution import durable_execution, wait
from typing import Any


@durable_execution
async def handler(_event: Any) -> str:
    await wait(timedelta(seconds=1))
    msg = "target function error"
    raise RuntimeError(msg)

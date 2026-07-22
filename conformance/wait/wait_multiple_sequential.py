"""2-3: Multiple sequential waits."""

from datetime import timedelta
from async_durable_execution import durable_execution, wait
from typing import Any


@durable_execution
async def handler(_event: Any) -> dict:
    await wait(timedelta(seconds=2), name="wait-1")
    await wait(timedelta(seconds=2), name="wait-2")
    return {"completedWaits": 2}

"""Target function that echoes back whatever it receives (with a short wait to ensure caller suspends)."""

from datetime import timedelta
from async_durable_execution import durable_execution, wait
from typing import Any


@durable_execution
async def handler(event: Any) -> Any:
    await wait(timedelta(seconds=1))
    return event

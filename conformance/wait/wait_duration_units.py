"""2-4: Wait with different duration units."""

from datetime import timedelta
from async_durable_execution import durable_execution, wait
from typing import Any


@durable_execution
async def handler(_event: Any) -> str:
    await wait(timedelta(minutes=1))
    return "Wait with minutes completed"

"""2-5: Wait with long duration (1 hour)."""

from datetime import timedelta
from async_durable_execution import durable_execution, wait
from typing import Any


@durable_execution
async def handler(_event: Any) -> str:
    await wait(timedelta(hours=1))
    return "Wait with hours completed"

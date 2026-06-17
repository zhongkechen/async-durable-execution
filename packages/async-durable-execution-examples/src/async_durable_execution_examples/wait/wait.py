from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_execution,
    wait,
)


@durable_execution
async def handler(_event: Any) -> str:
    await wait(timedelta(seconds=1))
    return "Wait completed"

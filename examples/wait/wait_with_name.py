from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_execution,
    wait,
)


@durable_execution
async def handler(_event: Any) -> str:
    # Wait with explicit name
    await wait(timedelta(seconds=1), name="custom_wait")
    return "Wait with name completed"

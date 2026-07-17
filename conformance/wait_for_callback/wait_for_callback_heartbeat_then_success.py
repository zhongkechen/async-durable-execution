"""7-13: Wait-for-callback heartbeat then success."""

from datetime import timedelta
from typing import Any

from async_durable_execution import durable_execution, wait_for_callback


async def submitter() -> None:
    pass


@durable_execution
async def handler(event: Any) -> str:
    return await wait_for_callback(
        submitter, name=event, heartbeat_timeout=timedelta(seconds=10)
    )

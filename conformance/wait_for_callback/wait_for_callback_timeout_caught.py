"""7-14: Wait-for-callback timeout caught."""

from datetime import timedelta
from typing import Any

from async_durable_execution import durable_execution, wait_for_callback


async def submitter() -> None:
    pass


@durable_execution
async def handler(event: Any) -> str:
    try:
        return await wait_for_callback(
            submitter, name=event, timeout=timedelta(seconds=3)
        )
    except Exception:
        return "timed-out-handled"

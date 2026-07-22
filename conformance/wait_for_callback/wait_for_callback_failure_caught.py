"""7-6: Wait-for-callback external failure caught."""

from typing import Any

from async_durable_execution import durable_execution, wait_for_callback


async def submitter() -> None:
    pass


@durable_execution
async def handler(event: Any) -> str:
    try:
        return await wait_for_callback(submitter, name=event)
    except Exception:
        return "recovered"

"""7-15: Wait-for-callback with a null result."""

from typing import Any

from async_durable_execution import durable_execution, wait_for_callback


async def submitter() -> None:
    pass


@durable_execution
async def handler(event: Any) -> Any:
    return await wait_for_callback(submitter, name=event)

"""7-2: Wait-for-callback with an explicit name."""

from typing import Any

from async_durable_execution import durable_execution, wait_for_callback


async def submitter() -> None:
    pass


@durable_execution
async def handler(_event: Any) -> str:
    return await wait_for_callback(submitter, name="approval")

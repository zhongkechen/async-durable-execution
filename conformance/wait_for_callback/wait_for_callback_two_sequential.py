"""7-9: Two sequential wait-for-callback operations."""

from typing import Any

from async_durable_execution import durable_execution, wait_for_callback


async def submitter() -> None:
    pass


@durable_execution
async def handler(_event: Any) -> str:
    await wait_for_callback(submitter, name="first")
    return await wait_for_callback(submitter, name="second")

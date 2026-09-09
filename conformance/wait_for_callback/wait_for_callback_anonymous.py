"""7-3: Wait-for-callback without an operation name."""

from typing import Any

from async_durable_execution import durable_execution, wait_for_callback


class AnonymousSubmitter:
    async def __call__(self) -> None:
        pass


@durable_execution
async def handler(_event: Any) -> str:
    return await wait_for_callback(AnonymousSubmitter())

"""Demonstrates waitForCallback with anonymous (inline) submitter function."""

import asyncio
from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    wait_for_callback,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with anonymous submitter."""

    @durable_callable
    async def submitter() -> None:
        await asyncio.sleep(0.05)

    result: str = await wait_for_callback(submitter())

    return {
        "callbackResult": result,
        "completed": True,
    }

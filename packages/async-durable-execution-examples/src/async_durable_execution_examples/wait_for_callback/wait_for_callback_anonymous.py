"""Demonstrates waitForCallback with anonymous (inline) submitter function."""

import asyncio
from typing import Any

from async_durable_execution import (
    durable_execution,
    wait_for_callback,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with anonymous submitter."""

    async def submitter(_callback_id, _context) -> None:
        await asyncio.sleep(1)

    result: str = await wait_for_callback(submitter)

    return {
        "callbackResult": result,
        "completed": True,
    }

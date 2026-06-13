"""Demonstrates waitForCallback with anonymous (inline) submitter function."""

import asyncio
from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with anonymous submitter."""

    async def submitter(_callback_id, _context) -> None:
        await asyncio.sleep(1)

    result: str = await context.wait_for_callback(submitter)

    return {
        "callbackResult": result,
        "completed": True,
    }

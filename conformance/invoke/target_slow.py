"""Target function that waits longer than timeout."""

import asyncio
from async_durable_execution import durable_execution
from typing import Any


@durable_execution
async def handler(_event: Any) -> str:
    await asyncio.sleep(60)
    return "should not reach here"

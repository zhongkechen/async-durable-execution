"""Demonstrates sending heartbeats during long-running callback processing."""

import asyncio
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    WaitForCallbackConfig,
    durable_execution,
    wait_for_callback,
)


async def submitter(_callback_id: str) -> None:
    """Simulate long-running submitter function."""
    await asyncio.sleep(0.05)


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with heartbeat timeout."""

    config = WaitForCallbackConfig(
        timeout=timedelta(seconds=30), heartbeat_timeout=timedelta(seconds=5)
    )

    result: str = await wait_for_callback(submitter, config=config)

    return {
        "callbackResult": result,
        "completed": True,
    }

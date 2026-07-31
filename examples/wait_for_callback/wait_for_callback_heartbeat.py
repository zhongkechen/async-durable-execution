"""Demonstrates sending heartbeats during long-running callback processing."""

import asyncio
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    wait_for_callback,
)


@durable_callable
async def submitter() -> None:
    """Simulate long-running submitter function."""
    await asyncio.sleep(0.05)


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with heartbeat timeout."""

    result: str = await wait_for_callback(
        submitter(),
        timeout=timedelta(seconds=30),
        heartbeat_timeout=timedelta(seconds=5),
    )

    return {
        "callbackResult": result,
        "completed": True,
    }

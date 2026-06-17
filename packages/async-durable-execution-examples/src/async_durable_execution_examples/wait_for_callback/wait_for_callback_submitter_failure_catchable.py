"""Demonstrates waitForCallback with submitter function that fails."""

import asyncio
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    WaitForCallbackConfig,
    durable_execution,
    RetryStrategyBuilder,
    wait_for_callback,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with failing submitter."""

    async def submitter(_callback_id) -> None:
        """Submitter function that fails after a delay."""
        await asyncio.sleep(0.05)
        # Submitter fails
        raise Exception("Submitter failed")

    config = WaitForCallbackConfig(
        timeout=timedelta(seconds=3),
        heartbeat_timeout=timedelta(seconds=3),
        retry_strategy=RetryStrategyBuilder(
            max_attempts=3,
            initial_delay=timedelta(seconds=1),
            max_delay=timedelta(seconds=1),
        ).build(),
    )

    try:
        result: str = await wait_for_callback(
            submitter,
            name="failing-submitter-callback",
            config=config,
        )

        return {
            "callbackResult": result,
            "success": True,
        }
    except Exception as error:
        return {
            "success": False,
            "error": str(error),
        }

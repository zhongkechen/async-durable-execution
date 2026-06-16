"""Demonstrates waitForCallback with submitter function that fails."""

import asyncio
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    WaitForCallbackConfig,
    durable_execution,
    RetryStrategyConfig,
    create_retry_strategy,
    wait_for_callback,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with failing submitter."""

    async def submitter(_callback_id, _context) -> None:
        """Submitter function that fails after a delay."""
        await asyncio.sleep(0.5)
        # Submitter fails
        raise Exception("Submitter failed")

    config = WaitForCallbackConfig(
        timeout=timedelta(seconds=10),
        heartbeat_timeout=timedelta(seconds=20),
        retry_strategy=create_retry_strategy(
            config=RetryStrategyConfig(
                max_attempts=3,
                initial_delay=timedelta(seconds=1),
                max_delay=timedelta(seconds=1),
            )
        ),
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

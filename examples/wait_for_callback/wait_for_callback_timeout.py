"""Demonstrates waitForCallback timeout scenarios."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    wait_for_callback,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating waitForCallback timeout."""

    @durable_callable
    async def submitter() -> None:
        """Submitter succeeds but callback never completes."""
        return None

    try:
        result: str = await wait_for_callback(
            submitter(),
            timeout=timedelta(seconds=1),
            heartbeat_timeout=timedelta(seconds=2),
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

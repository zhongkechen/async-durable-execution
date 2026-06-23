"""Demonstrates waitForCallback timeout scenarios."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    LambdaContext,
    WaitForCallbackConfig,
    durable_execution,
    wait_for_callback,
)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback timeout."""

    config = WaitForCallbackConfig(
        timeout=timedelta(seconds=1), heartbeat_timeout=timedelta(seconds=2)
    )

    async def submitter(_callback_id) -> None:
        """Submitter succeeds but callback never completes."""
        return None

    try:
        result: str = await wait_for_callback(
            submitter,
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

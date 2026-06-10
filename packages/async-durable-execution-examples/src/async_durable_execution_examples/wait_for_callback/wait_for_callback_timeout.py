"""Demonstrates waitForCallback timeout scenarios."""

from typing import Any

from async_durable_execution.config import Duration, WaitForCallbackConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback timeout."""

    config = WaitForCallbackConfig(
        timeout=Duration.from_seconds(1), heartbeat_timeout=Duration.from_seconds(2)
    )

    async def submitter(_callback_id, _context) -> None:
        """Submitter succeeds but callback never completes."""
        return None

    try:
        result: str = context.wait_for_callback(
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

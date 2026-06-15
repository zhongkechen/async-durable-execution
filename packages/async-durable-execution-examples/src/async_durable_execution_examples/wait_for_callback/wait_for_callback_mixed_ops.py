"""Demonstrates waitForCallback combined with steps, waits, and other operations."""

import asyncio
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    durable_execution,
    wait,
    wait_for_callback,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating waitForCallback mixed with other operations."""
    # Mix waitForCallback with other operation types
    await wait(timedelta(seconds=1), name="initial-wait")

    @durable_step
    async def fetch_user_data() -> dict[str, Any]:
        return {"userId": 123, "name": "John Doe"}

    step_result: dict[str, Any] = await step(
        fetch_user_data(),
        name="fetch-user-data",
    )

    async def submitter(_callback_id, _context) -> None:
        """Submitter uses data from previous step."""
        await asyncio.sleep(0.1)

    callback_result: str = await wait_for_callback(
        submitter,
        name="wait-for-callback",
    )

    await wait(timedelta(seconds=2), name="final-wait")

    @durable_step
    async def finalize_processing() -> dict[str, Any]:
        return {
            "status": "completed",
            "timestamp": 1_717_894_400_000,
        }

    final_step: dict[str, Any] = await step(
        finalize_processing(),
        name="finalize-processing",
    )

    return {
        "stepResult": step_result,
        "callbackResult": callback_result,
        "finalStep": final_step,
        "workflowCompleted": True,
    }

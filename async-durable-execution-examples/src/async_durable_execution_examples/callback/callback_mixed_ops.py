"""Demonstrates createCallback mixed with steps, waits, and other operations."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    step,
    durable_execution,
    create_callback,
    wait,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating createCallback mixed with other operations."""

    @durable_callable
    async def fetch_data() -> dict[str, Any]:
        return {"userId": 123, "name": "John Doe"}

    step_result: dict[str, Any] = await step(
        fetch_data(),
        name="fetch-data",
    )

    callback = await create_callback(
        name="process-user",
        timeout=timedelta(minutes=1),
    )

    # Mix callback with step and wait operations
    await wait(timedelta(seconds=1), name="initial-wait")

    callback_result = await callback.result()

    return {
        "stepResult": step_result,
        "callbackResult": callback_result,
        "completed": True,
    }

"""Demonstrates createCallback mixed with steps, waits, and other operations."""

from datetime import timedelta
from typing import Any

from async_durable_execution.config import CallbackConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating createCallback mixed with other operations."""

    async def fetch_data(_) -> dict[str, Any]:
        return {"userId": 123, "name": "John Doe"}

    step_result: dict[str, Any] = context.step(
        fetch_data,
        name="fetch-data",
    )

    callback_config = CallbackConfig(timeout=timedelta(minutes=1))
    callback = context.create_callback(
        name="process-user",
        config=callback_config,
    )

    # Mix callback with step and wait operations
    context.wait(timedelta(seconds=1), name="initial-wait")

    callback_result = callback.result()

    return {
        "stepResult": step_result,
        "callbackResult": callback_result,
        "completed": True,
    }

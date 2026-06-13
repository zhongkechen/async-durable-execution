"""Demonstrates step execution tracking when no replay occurs."""

from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, bool]:
    """Handler demonstrating step execution without replay."""

    async def fetch_user_1(_) -> str:
        return "user-1"

    async def fetch_user_2(_) -> str:
        return "user-2"

    await context.step(fetch_user_1, name="fetch-user-1")
    await context.step(fetch_user_2, name="fetch-user-2")

    return {"completed": True}

"""Demonstrates step execution tracking when no replay occurs."""

from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_callable,
    step,
    durable_execution,
)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> dict[str, bool]:
    """Handler demonstrating step execution without replay."""

    @durable_callable
    async def fetch_user_1() -> str:
        return "user-1"

    @durable_callable
    async def fetch_user_2() -> str:
        return "user-2"

    await step(fetch_user_1(), name="fetch-user-1")
    await step(fetch_user_2(), name="fetch-user-2")

    return {"completed": True}

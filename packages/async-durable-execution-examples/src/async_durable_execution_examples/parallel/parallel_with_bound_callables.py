"""Example demonstrating parallel with bound durable callables."""

import asyncio
from typing import Any

from async_durable_execution import (
    LambdaContext,
    ParallelConfig,
    durable_callable,
    durable_execution,
    parallel,
    step,
)


@durable_callable
async def fetch_orders(user_id: str) -> str:
    await asyncio.sleep(0)

    @durable_callable
    async def load_orders() -> str:
        return f"orders-loaded-{user_id}"

    return await step(load_orders(), name="load_orders")


@durable_callable
async def fetch_preferences(user_id: str) -> str:
    await asyncio.sleep(0)

    @durable_callable
    async def load_prefs() -> str:
        return f"prefs-loaded-{user_id}"

    return await step(load_prefs(), name="load_prefs")


@durable_execution
async def handler(event: dict[str, Any], context: LambdaContext) -> list[str]:
    """Execute parallel branches using bound durable callables."""
    user_id = event.get("user_id", "user-123")

    @durable_callable
    async def fetch_user_data(user_id: str) -> str:
        @durable_callable
        async def load_user() -> str:
            return f"user-data-loaded-{user_id}"

        return await step(load_user(), name="load_user")

    @durable_callable
    async def fetch_metrics(user_id: str, metric: str) -> str:
        @durable_callable
        async def load_metrics() -> str:
            return f"{metric}-metrics-loaded-{user_id}"

        return await step(load_metrics(), name="load_metrics")

    @durable_callable
    async def load_config(region: str = "global") -> str:
        @durable_callable
        async def load_value() -> str:
            return f"config-loaded-{region}"

        return await step(load_value(), name="load_config")

    return (
        await parallel(
            branches=[
                fetch_user_data(user_id),
                fetch_orders(user_id),
                fetch_preferences(user_id),
                fetch_metrics(user_id, "usage"),
                load_config(region="us-east-1"),
            ],
            name="load_all_data",
            config=ParallelConfig(max_concurrency=3),
        )
    ).get_results()

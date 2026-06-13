"""Example demonstrating all parallel branch patterns."""

import asyncio
from typing import Any

from async_durable_execution.config import ParallelBranch, ParallelConfig
from async_durable_execution.context import (
    DurableContext,
    durable_parallel_branch,
)
from async_durable_execution.execution import durable_execution


@durable_parallel_branch(name="fetch-orders")
async def fetch_orders(ctx: DurableContext) -> str:
    await asyncio.sleep(0)

    async def load_orders(_) -> str:
        return "orders-loaded"

    return await ctx.step(load_orders, name="load_orders")


@durable_parallel_branch()
async def fetch_preferences(ctx: DurableContext) -> str:
    await asyncio.sleep(0)

    async def load_prefs(_) -> str:
        return "prefs-loaded"

    return await ctx.step(load_prefs, name="load_prefs")


@durable_execution
async def handler(_event: Any, context: DurableContext) -> list[str]:
    """Execute parallel branches using all supported patterns."""

    async def fetch_user_data(ctx: DurableContext) -> str:
        async def load_user(_) -> str:
            return "user-data-loaded"

        return await ctx.step(load_user, name="load_user")

    async def fetch_metrics(ctx: DurableContext) -> str:
        async def load_metrics(_) -> str:
            return "metrics-loaded"

        return await ctx.step(load_metrics, name="load_metrics")

    async def load_config(ctx: DurableContext) -> str:
        async def load_value(_) -> str:
            return "config-loaded"

        return await ctx.step(load_value, name="load_config")

    return (
        await context.parallel(
            functions=[
                # 1. Named parallel branch with ParallelBranch
                ParallelBranch(
                    func=fetch_user_data,
                    name="fetch-user-data",
                ),
                # 2. Named parallel branch with decorator
                fetch_orders(),
                # 3. Unnamed parallel branch with decorator
                fetch_preferences(),
                # 4. Unnamed parallel branch with ParallelBranch
                ParallelBranch(
                    func=fetch_metrics,
                ),
                # 5. No wrapper, just a raw callable
                load_config,
            ],
            name="load_all_data",
            config=ParallelConfig(max_concurrency=3),
        )
    ).get_results()

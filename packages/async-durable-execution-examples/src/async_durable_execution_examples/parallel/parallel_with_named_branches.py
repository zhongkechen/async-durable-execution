"""Example demonstrating all parallel branch patterns."""

import asyncio
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    ParallelBranch,
    ParallelConfig,
    durable_parallel_branch,
    durable_execution,
    parallel,
)


@durable_parallel_branch(name="fetch-orders")
async def fetch_orders() -> str:
    await asyncio.sleep(0)

    @durable_step
    async def load_orders() -> str:
        return "orders-loaded"

    return await step(load_orders(), name="load_orders")


@durable_parallel_branch()
async def fetch_preferences() -> str:
    await asyncio.sleep(0)

    @durable_step
    async def load_prefs() -> str:
        return "prefs-loaded"

    return await step(load_prefs(), name="load_prefs")


@durable_execution
async def handler(_event: Any) -> list[str]:
    """Execute parallel branches using all supported patterns."""

    async def fetch_user_data() -> str:
        @durable_step
        async def load_user() -> str:
            return "user-data-loaded"

        return await step(load_user(), name="load_user")

    async def fetch_metrics() -> str:
        @durable_step
        async def load_metrics() -> str:
            return "metrics-loaded"

        return await step(load_metrics(), name="load_metrics")

    async def load_config() -> str:
        @durable_step
        async def load_value() -> str:
            return "config-loaded"

        return await step(load_value(), name="load_config")

    return (
        await parallel(
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

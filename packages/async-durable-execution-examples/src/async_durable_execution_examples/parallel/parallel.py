"""Example demonstrating parallel operations for concurrent execution."""

from datetime import timedelta
from typing import Any

from async_durable_execution.config import ParallelConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> list[str]:
    """Execute multiple operations in parallel using context.parallel()."""

    async def task1(ctx: DurableContext) -> str:
        async def run() -> str:
            return "task 1 completed"

        return await ctx.step(run, name="task1")

    async def task2(ctx: DurableContext) -> str:
        async def run() -> str:
            return "task 2 completed"

        return await ctx.step(run, name="task2")

    async def task3(ctx: DurableContext) -> str:
        await ctx.wait(timedelta(seconds=1), name="wait_in_task3")
        return "task 3 completed after wait"

    # Use context.parallel() to execute functions concurrently and extract results immediately
    return (
        await context.parallel(
            functions=[task1, task2, task3],
            name="parallel_operation",
            config=ParallelConfig(max_concurrency=2),
        )
    ).get_results()

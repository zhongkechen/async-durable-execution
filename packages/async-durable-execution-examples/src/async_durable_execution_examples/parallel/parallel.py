"""Example demonstrating parallel operations for concurrent execution."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    ParallelConfig,
    durable_execution,
    parallel,
    wait,
)


@durable_execution
async def handler(_event: Any) -> list[str]:
    """Execute multiple operations in parallel using parallel()."""

    async def task1() -> str:
        @durable_step
        async def run() -> str:
            return "task 1 completed"

        return await step(run(), name="task1")

    async def task2() -> str:
        @durable_step
        async def run() -> str:
            return "task 2 completed"

        return await step(run(), name="task2")

    async def task3() -> str:
        await wait(timedelta(seconds=1), name="wait_in_task3")
        return "task 3 completed after wait"

    # Use parallel() to execute functions concurrently and extract results immediately
    return (
        await parallel(
            functions=[task1, task2, task3],
            name="parallel_operation",
            config=ParallelConfig(max_concurrency=2),
        )
    ).get_results()

"""Example demonstrating parallel with maxConcurrency limit."""

from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_callable,
    step,
    ParallelConfig,
    durable_execution,
    parallel,
)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> list[str]:
    """Execute 5 tasks with concurrency limit of 2."""

    async def task1() -> str:
        @durable_callable
        async def run() -> str:
            return "task 1"

        return await step(run(), name="task1")

    async def task2() -> str:
        @durable_callable
        async def run() -> str:
            return "task 2"

        return await step(run(), name="task2")

    async def task3() -> str:
        @durable_callable
        async def run() -> str:
            return "task 3"

        return await step(run(), name="task3")

    async def task4() -> str:
        @durable_callable
        async def run() -> str:
            return "task 4"

        return await step(run(), name="task4")

    async def task5() -> str:
        @durable_callable
        async def run() -> str:
            return "task 5"

        return await step(run(), name="task5")

    # Extract results immediately to avoid BatchResult serialization
    return (
        await parallel(
            functions=[task1, task2, task3, task4, task5],
            name="parallel_with_concurrency",
            config=ParallelConfig(max_concurrency=2),
        )
    ).get_results()

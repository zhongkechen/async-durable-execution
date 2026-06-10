"""Example demonstrating parallel with maxConcurrency limit."""

from typing import Any

from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> list[str]:
    """Execute 5 tasks with concurrency limit of 2."""

    async def task1(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return "task 1"

        return ctx.step(run, name="task1")

    async def task2(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return "task 2"

        return ctx.step(run, name="task2")

    async def task3(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return "task 3"

        return ctx.step(run, name="task3")

    async def task4(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return "task 4"

        return ctx.step(run, name="task4")

    async def task5(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return "task 5"

        return ctx.step(run, name="task5")

    # Extract results immediately to avoid BatchResult serialization
    return context.parallel(
        functions=[task1, task2, task3, task4, task5],
        name="parallel_with_concurrency",
        config=ParallelConfig(max_concurrency=2),
    ).get_results()

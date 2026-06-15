from typing import Any

from async_durable_execution.config import CompletionConfig, ParallelConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    # Parallel execution with first_successful completion strategy
    config = ParallelConfig(completion_config=CompletionConfig.first_successful())

    async def task1(ctx: DurableContext) -> str:
        async def run() -> str:
            return "Task 1"

        return await ctx.step(run, name="task1")

    async def task2(ctx: DurableContext) -> str:
        async def run() -> str:
            return "Task 2"

        return await ctx.step(run, name="task2")

    async def task3(ctx: DurableContext) -> str:
        async def run() -> str:
            return "Task 3"

        return await ctx.step(run, name="task3")

    functions = [task1, task2, task3]

    results = await context.parallel(
        functions, name="first_successful_parallel", config=config
    )

    # Extract the first successful result
    successful = results.get_results()
    first_result = successful[0] if successful else "None"
    return f"First successful result: {first_result}"

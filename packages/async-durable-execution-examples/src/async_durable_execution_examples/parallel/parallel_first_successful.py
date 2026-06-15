from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    CompletionConfig,
    ParallelConfig,
    durable_execution,
    parallel,
)


@durable_execution
async def handler(_event: Any) -> str:
    # Parallel execution with first_successful completion strategy
    config = ParallelConfig(completion_config=CompletionConfig.first_successful())

    async def task1() -> str:
        @durable_step
        async def run() -> str:
            return "Task 1"

        return await step(run(), name="task1")

    async def task2() -> str:
        @durable_step
        async def run() -> str:
            return "Task 2"

        return await step(run(), name="task2")

    async def task3() -> str:
        @durable_step
        async def run() -> str:
            return "Task 3"

        return await step(run(), name="task3")

    functions = [task1, task2, task3]

    results = await parallel(functions, name="first_successful_parallel", config=config)

    # Extract the first successful result
    successful = results.get_results()
    first_result = successful[0] if successful else "None"
    return f"First successful result: {first_result}"

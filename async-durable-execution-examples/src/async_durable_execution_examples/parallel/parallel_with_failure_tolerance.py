"""Example demonstrating parallel with failure tolerance."""

from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_callable,
    step,
    CompletionConfig,
    ParallelConfig,
    StepConfig,
    durable_execution,
    RetryStrategyBuilder,
    parallel,
)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> dict[str, Any]:
    """Execute tasks with failure tolerance."""

    # Tolerate up to 2 failures
    config = ParallelConfig(
        completion_config=CompletionConfig(tolerated_failure_count=2)
    )

    # Disable retries so failures happen immediately
    step_config = StepConfig(
        retry_strategy=RetryStrategyBuilder(max_attempts=1).build()
    )

    async def task1() -> str:
        @durable_callable
        async def run() -> str:
            return "success 1"

        return await step(run(), name="task1", config=step_config)

    async def task2() -> str:
        @durable_callable
        async def run() -> str:
            return await _failing_task(2)

        return await step(run(), name="task2", config=step_config)

    async def task3() -> str:
        @durable_callable
        async def run() -> str:
            return "success 3"

        return await step(run(), name="task3", config=step_config)

    async def task4() -> str:
        @durable_callable
        async def run() -> str:
            return await _failing_task(4)

        return await step(run(), name="task4", config=step_config)

    async def task5() -> str:
        @durable_callable
        async def run() -> str:
            return "success 5"

        return await step(run(), name="task5", config=step_config)

    results = await parallel(
        branches=[task1, task2, task3, task4, task5],
        name="parallel_with_tolerance",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "failure_count": results.failure_count,
        "succeeded": results.get_results(),
        "completion_reason": results.completion_reason.value,
    }


async def _failing_task(task_num: int) -> str:
    """Task that always fails."""
    raise ValueError(f"Task {task_num} failed")

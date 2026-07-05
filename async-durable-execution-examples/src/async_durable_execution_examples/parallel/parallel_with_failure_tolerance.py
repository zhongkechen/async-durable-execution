"""Example demonstrating parallel with failure tolerance."""

from typing import Any

from async_durable_execution import (
    durable_callable,
    step,
    CompletionConfig,
    durable_execution,
    RetryStrategy,
    parallel,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Execute tasks with failure tolerance."""

    # Tolerate up to 2 failures
    completion_config = CompletionConfig(tolerated_failure_count=2)

    # Disable retries so failures happen immediately
    retry_strategy = RetryStrategy(max_attempts=1)

    async def task1() -> str:
        @durable_callable
        async def run() -> str:
            return "success 1"

        return await step(run(), name="task1", retry_strategy=retry_strategy)

    async def task2() -> str:
        @durable_callable
        async def run() -> str:
            return await _failing_task(2)

        return await step(run(), name="task2", retry_strategy=retry_strategy)

    async def task3() -> str:
        @durable_callable
        async def run() -> str:
            return "success 3"

        return await step(run(), name="task3", retry_strategy=retry_strategy)

    async def task4() -> str:
        @durable_callable
        async def run() -> str:
            return await _failing_task(4)

        return await step(run(), name="task4", retry_strategy=retry_strategy)

    async def task5() -> str:
        @durable_callable
        async def run() -> str:
            return "success 5"

        return await step(run(), name="task5", retry_strategy=retry_strategy)

    results = await parallel(
        branches=[task1, task2, task3, task4, task5],
        name="parallel_with_tolerance",
        completion_config=completion_config,
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

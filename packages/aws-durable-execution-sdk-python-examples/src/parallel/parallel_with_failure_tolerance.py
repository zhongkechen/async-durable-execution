"""Example demonstrating parallel with failure tolerance."""

from typing import Any

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    ParallelConfig,
    StepConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryStrategyConfig


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Execute tasks with failure tolerance."""

    # Tolerate up to 2 failures
    config = ParallelConfig(
        completion_config=CompletionConfig(tolerated_failure_count=2)
    )

    # Disable retries so failures happen immediately
    step_config = StepConfig(retry_strategy=RetryStrategyConfig(max_attempts=1))

    async def task1(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return "success 1"

        return ctx.step(run, name="task1", config=step_config)

    async def task2(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return await _failing_task(2)

        return ctx.step(run, name="task2", config=step_config)

    async def task3(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return "success 3"

        return ctx.step(run, name="task3", config=step_config)

    async def task4(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return await _failing_task(4)

        return ctx.step(run, name="task4", config=step_config)

    async def task5(ctx: DurableContext) -> str:
        async def run(_) -> str:
            return "success 5"

        return ctx.step(run, name="task5", config=step_config)

    results = context.parallel(
        functions=[task1, task2, task3, task4, task5],
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

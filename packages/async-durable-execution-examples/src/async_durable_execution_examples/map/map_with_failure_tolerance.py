"""Example demonstrating map with failure tolerance."""

import asyncio
from typing import Any

from async_durable_execution.config import (
    CompletionConfig,
    MapConfig,
    StepConfig,
)
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution
from async_durable_execution.retries import RetryStrategyConfig


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Process items with failure tolerance."""
    items = list(range(1, 11))  # [1, 2, 3, ..., 10]

    # Tolerate up to 3 failures
    config = MapConfig(
        max_concurrency=5,
        completion_config=CompletionConfig(tolerated_failure_count=3),
    )

    # Disable retries so failures happen immediately
    step_config = StepConfig(retry_strategy=RetryStrategyConfig(max_attempts=1))

    async def process_item(ctx: DurableContext, item: int, index: int, _) -> int:
        await asyncio.sleep(0)

        async def run(_) -> int:
            return await _process_with_failures(item)

        return ctx.step(
            run,
            name=f"item_{index}",
            config=step_config,
        )

    results = context.map(
        inputs=items,
        func=process_item,
        name="map_with_tolerance",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "failure_count": results.failure_count,
        "succeeded": [item.result for item in results.succeeded()],
        "failed_count": len(results.failed()),
        "completion_reason": results.completion_reason.value,
    }


async def _process_with_failures(item: int) -> int:
    """Process item - fails for items 3, 6, 9."""
    if item % 3 == 0:
        raise ValueError(f"Item {item} failed")
    return item * 2

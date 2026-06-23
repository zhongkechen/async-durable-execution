"""Example demonstrating parallel with custom serdes."""

import json
from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_callable,
    step,
    ParallelConfig,
    durable_execution,
    SerDes,
    SerDesContext,
    parallel,
)


class CustomItemSerDes(SerDes[dict[str, Any]]):
    """Custom serializer for individual items that adds metadata."""

    def serialize(self, value: dict[str, Any], _: SerDesContext) -> str:
        # Add custom metadata during serialization
        wrapped = {"data": value, "serialized_by": "CustomItemSerDes"}

        return json.dumps(wrapped)

    def deserialize(self, payload: str, _: SerDesContext) -> dict[str, Any]:
        wrapped = json.loads(payload)
        # Extract the original data
        return wrapped["data"]


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> dict[str, Any]:
    """Execute parallel tasks with custom item serialization.

    This example demonstrates using item_serdes to customize serialization
    of individual function results, while using default serialization for the
    overall BatchResult.
    """

    # Use custom serdes for individual function results only
    # The BatchResult will use default JSON serialization
    config = ParallelConfig(item_serdes=CustomItemSerDes())

    async def task1() -> dict[str, Any]:
        @durable_callable
        async def run() -> dict[str, Any]:
            return {"task": "task1", "value": 100}

        return await step(run(), name="task1")

    async def task2() -> dict[str, Any]:
        @durable_callable
        async def run() -> dict[str, Any]:
            return {"task": "task2", "value": 200}

        return await step(run(), name="task2")

    async def task3() -> dict[str, Any]:
        @durable_callable
        async def run() -> dict[str, Any]:
            return {"task": "task3", "value": 300}

        return await step(run(), name="task3")

    results = await parallel(
        branches=[task1, task2, task3],
        name="parallel_with_custom_serdes",
        config=config,
    )

    return {
        "success_count": results.success_count,
        "results": results.get_results(),
        "total_value": sum(r["value"] for r in results.get_results()),
    }

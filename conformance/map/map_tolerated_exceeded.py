"""9-9: Map failure tolerance exceeded."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, map

from map._helpers import summary


async def map_fn(item: str) -> str:
    if item != "never":
        raise RuntimeError("item failed")
    return item


@durable_execution
async def handler(_event: Any) -> dict:
    result = await map(
        map_fn,
        ["f0", "f1", "never"],
        name="tolerated-exceeded",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=1),
    )
    return summary(result)

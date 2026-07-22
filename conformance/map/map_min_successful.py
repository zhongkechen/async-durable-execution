"""9-7: Map minimum-successful early completion."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, map

from map._helpers import summary


async def map_fn(item: str) -> str:
    return item


@durable_execution
async def handler(_event: Any) -> dict:
    result = await map(
        map_fn,
        ["s0", "s1", "s2", "s3"],
        name="min-successful",
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=2),
    )
    projected = summary(result)
    return {
        "completionReason": projected["completionReason"],
        "successCount": projected["successCount"],
        "totalCount": projected["totalCount"],
    }

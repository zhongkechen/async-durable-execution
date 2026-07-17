"""8-9: Parallel failure within tolerance."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import fail, summary


async def first() -> str:
    return "s0"


async def third() -> str:
    return "s2"


@durable_execution
async def handler(_event: Any) -> dict:
    result = await parallel(
        [first, fail, third],
        name="tolerated",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=1),
    )
    projected = summary(result)
    projected["status"] = result.status.value
    return projected

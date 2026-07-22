"""9-8: Map failure within tolerance."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, map

from map._helpers import fail_on, summary


@durable_execution
async def handler(_event: Any) -> dict:
    result = await map(
        fail_on,
        ["s0", "fail", "s2"],
        name="tolerated",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=1),
    )
    projected = summary(result)
    projected["status"] = result.status.value
    return projected

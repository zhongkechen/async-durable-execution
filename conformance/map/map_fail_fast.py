"""9-5: Map fail-fast."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, map

from map._helpers import fail_on, summary


@durable_execution
async def handler(_event: Any) -> dict:
    result = await map(
        fail_on,
        ["ok", "fail", "never"],
        name="failfast",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=0),
    )
    projected = summary(result)
    projected["status"] = result.status.value
    return projected

"""8-16: Parallel all branches fail within tolerance."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import fail, summary


@durable_execution
async def handler(_event: Any) -> dict:
    result = await parallel(
        [fail, fail, fail],
        name="all-fail",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=3),
    )
    projected = summary(result)
    projected["status"] = result.status.value
    return projected

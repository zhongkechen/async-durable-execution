"""8-17: Parallel minimum-successful threshold not reached."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import fail, summary


async def first() -> str:
    return "ok0"


async def third() -> str:
    return "ok2"


@durable_execution
async def handler(_event: Any) -> dict:
    result = await parallel(
        [first, fail, third],
        name="min-not-reached",
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=3, tolerated_failure_count=3),
    )
    projected = summary(result)
    projected["status"] = result.status.value
    return projected

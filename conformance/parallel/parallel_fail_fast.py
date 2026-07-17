"""8-6: Parallel fail-fast."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import fail, summary


async def ok() -> str:
    return "ok"


async def never() -> str:
    return "never"


@durable_execution
async def handler(_event: Any) -> dict:
    result = await parallel(
        [ok, fail, never],
        name="failfast",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=0),
    )
    projected = summary(result)
    projected["status"] = result.status.value
    return projected

"""8-20: Parallel BatchResult accessors."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import fail


async def first() -> str:
    return "ok0"


async def third() -> str:
    return "ok2"


@durable_execution
async def handler(_event: Any) -> dict:
    result = await parallel(
        [first, fail, third],
        name="accessors",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=1),
    )
    return {
        "hasFailure": result.has_failure,
        "successCount": len(result.succeeded()),
        "failureCount": len(result.failed()),
        "errorCount": len(result.get_errors()),
    }

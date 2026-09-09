"""8-10: Parallel failure tolerance exceeded."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import fail, summary


async def never() -> str:
    return "never"


@durable_execution
async def handler(_event: Any) -> dict:
    result = await parallel(
        [fail, fail, never],
        name="tolerated-exceeded",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=1),
    )
    return summary(result)

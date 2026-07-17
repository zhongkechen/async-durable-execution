"""8-18: Parallel combined completion config."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import fail, summary


async def ok2() -> str:
    return "ok2"


async def ok3() -> str:
    return "ok3"


@durable_execution
async def handler(_event: Any) -> dict:
    result = await parallel(
        [fail, fail, ok2, ok3],
        name="combined",
        max_concurrency=1,
        completion_config=CompletionConfig(min_successful=3, tolerated_failure_count=1),
    )
    return summary(result)

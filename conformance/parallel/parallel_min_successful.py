"""8-8: Parallel minimum-successful early completion."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import summary


def branch(value: str):
    async def run() -> str:
        return value

    return run


@durable_execution
async def handler(_event: Any) -> dict:
    result = await parallel(
        [branch("s0"), branch("s1"), branch("s2"), branch("s3")],
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

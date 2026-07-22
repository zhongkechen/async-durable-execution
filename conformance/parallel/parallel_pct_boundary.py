"""8-22: Parallel percentage tolerance boundary."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import fail, summary


def branch(value: str):
    async def run() -> str:
        return value

    return run


@durable_execution
async def handler(_event: Any) -> dict:
    result = await parallel(
        [fail, branch("ok1"), branch("ok2"), branch("ok3")],
        name="pct-boundary",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=1),
    )
    projected = summary(result)
    projected["status"] = result.status.value
    return projected

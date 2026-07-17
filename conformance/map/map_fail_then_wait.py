"""9-18: Wait after a map containing a tolerated failure."""

from datetime import timedelta
from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, map, wait

from map._helpers import fail_on, summary


@durable_execution
async def handler(_event: Any) -> dict:
    result = await map(
        fail_on,
        ["ok", "fail"],
        name="fail-then-wait",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=1),
    )
    await wait(timedelta(seconds=1))
    projected = summary(result)
    projected["status"] = result.status.value
    return projected

"""9-6: Map throw-if-error."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, map

from map._helpers import fail_on


@durable_execution
async def handler(_event: Any) -> list:
    result = await map(
        fail_on,
        ["fail", "never"],
        name="throwing",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=0),
    )
    result.throw_if_error()
    return result.get_results()

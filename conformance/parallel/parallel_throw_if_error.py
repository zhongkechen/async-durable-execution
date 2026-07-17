"""8-7: Parallel throw-if-error."""

from typing import Any

from async_durable_execution import CompletionConfig, durable_execution, parallel

from parallel._helpers import fail


async def never() -> str:
    return "never"


@durable_execution
async def handler(_event: Any) -> list:
    result = await parallel(
        [fail, never],
        name="throwing",
        max_concurrency=1,
        completion_config=CompletionConfig(tolerated_failure_count=0),
    )
    result.throw_if_error()
    return result.get_results()

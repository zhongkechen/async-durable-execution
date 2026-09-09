"""8-11: Parallel concurrent execution preserves result order."""

from collections.abc import Awaitable, Callable
from typing import Any

from async_durable_execution import durable_execution, parallel


def branch(value: str) -> Callable[[], Awaitable[str]]:
    async def run() -> str:
        return value

    return run


@durable_execution
async def handler(_event: Any) -> list:
    result = await parallel(
        [branch("r0"), branch("r1"), branch("r2")],
        name="concurrent",
        max_concurrency=2,
    )
    return result.get_results()

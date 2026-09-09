"""8-2: Parallel branches-only form."""

from typing import Any

from async_durable_execution import durable_execution, parallel


async def branch0() -> str:
    return "alpha"


async def branch1() -> str:
    return "beta"


@durable_execution
async def handler(_event: Any) -> list:
    return (await parallel([branch0, branch1], max_concurrency=1)).get_results()

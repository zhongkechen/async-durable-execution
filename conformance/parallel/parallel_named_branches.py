"""8-3: Parallel named branches."""

from typing import Any

from async_durable_execution import durable_execution, parallel


async def first() -> str:
    return "one"


async def second() -> str:
    return "two"


@durable_execution
async def handler(_event: Any) -> list:
    return (
        await parallel([first, second], name="named", max_concurrency=1)
    ).get_results()

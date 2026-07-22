"""8-19: Parallel invalid max concurrency."""

from typing import Any

from async_durable_execution import durable_execution, parallel


async def first() -> str:
    return "a"


async def second() -> str:
    return "b"


@durable_execution
async def handler(_event: Any) -> list:
    return (
        await parallel([first, second], name="bad-concurrency", max_concurrency=0)
    ).get_results()

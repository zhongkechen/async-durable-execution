"""8-4: Parallel heterogeneous branch results."""

from typing import Any

from async_durable_execution import durable_execution, parallel


async def string_branch() -> str:
    return "hello"


async def number_branch() -> int:
    return 42


async def object_branch() -> dict:
    return {"k": "v"}


@durable_execution
async def handler(_event: Any) -> list:
    result = await parallel(
        [string_branch, number_branch, object_branch],
        name="hetero",
        max_concurrency=1,
    )
    return result.get_results()

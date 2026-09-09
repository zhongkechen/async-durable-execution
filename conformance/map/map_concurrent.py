"""9-11: Concurrent map preserves result order."""

from typing import Any

from async_durable_execution import durable_execution, map


async def map_fn(item: str) -> str:
    return item


@durable_execution
async def handler(_event: Any) -> list:
    return (
        await map(
            map_fn,
            ["r0", "r1", "r2"],
            name="concurrent",
            max_concurrency=2,
        )
    ).get_results()

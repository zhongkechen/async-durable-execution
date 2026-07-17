"""9-2: Map items-only form."""

from typing import Any

from async_durable_execution import durable_execution, map


async def map_fn(item: int) -> int:
    return item * 2


@durable_execution
async def handler(event: Any) -> list:
    items = event if isinstance(event, list) else [1, 2]
    return (await map(map_fn, items, max_concurrency=1)).get_results()

"""9-3: Map function receives item and index."""

from typing import Any

from async_durable_execution import (
    durable_execution,
    get_map_item_context,
    map,
)


async def map_fn(item: int) -> int:
    context = get_map_item_context()
    return item + context.index


@durable_execution
async def handler(event: Any) -> list:
    items = event if isinstance(event, list) else [10, 20, 30]
    return (await map(map_fn, items, name="indexed", max_concurrency=1)).get_results()

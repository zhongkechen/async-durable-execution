"""9-13: Map with a custom item namer."""

from typing import Any

from async_durable_execution import durable_execution, map


async def map_fn(item: int) -> int:
    return item * 10


@durable_execution
async def handler(event: Any) -> list:
    items = event if isinstance(event, list) else [1, 2]
    result = await map(
        map_fn,
        items,
        name="named-items",
        max_concurrency=1,
        item_namer=lambda item, _index: f"item-{item}",
    )
    return result.get_results()

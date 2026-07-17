"""9-4: Map with no items."""

from typing import Any

from async_durable_execution import durable_execution, map


async def map_fn(item: Any) -> Any:
    return item


@durable_execution
async def handler(event: Any) -> list:
    items = event if isinstance(event, list) else []
    return (await map(map_fn, items, name="empty")).get_results()

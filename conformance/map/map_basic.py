"""9-1: Map basic."""

from typing import Any

from async_durable_execution import durable_callable, durable_execution, map, step


@durable_callable
async def greet(item: str) -> str:
    return f"Hello, {item}!"


async def map_fn(item: str) -> str:
    return await step(greet(item))


@durable_execution
async def handler(event: Any) -> list:
    items = event if isinstance(event, list) else ["World", "Kiro"]
    return (await map(map_fn, items, name="map", max_concurrency=1)).get_results()

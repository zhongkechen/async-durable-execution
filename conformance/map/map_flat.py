"""9-12: Map with flat nesting."""

from typing import Any

from async_durable_execution import (
    NestingType,
    durable_callable,
    durable_execution,
    map,
    step,
)


@durable_callable
async def return_value(value: str) -> str:
    return value


async def map_fn(item: str) -> str:
    return await step(return_value(item))


@durable_execution
async def handler(_event: Any) -> list:
    result = await map(
        map_fn,
        ["fa", "fb"],
        name="flat",
        max_concurrency=1,
        nesting_type=NestingType.FLAT,
    )
    return result.get_results()

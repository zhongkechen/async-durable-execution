"""9-15: Map suspends inside an iteration."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    MapItemContext,
    durable_callable,
    durable_execution,
    get_current_context,
    map,
    step,
    wait,
)


@durable_callable
async def return_value(value: str) -> str:
    return value


async def map_fn(item: str) -> str:
    context: MapItemContext = get_current_context()
    if context.index == 1:
        await wait(timedelta(seconds=1))
    return await step(return_value(item))


@durable_execution
async def handler(_event: Any) -> list:
    result = await map(map_fn, ["r0", "r1"], name="suspend", max_concurrency=1)
    return result.get_results()

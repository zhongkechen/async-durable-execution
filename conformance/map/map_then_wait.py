"""9-17: Wait after a successful map."""

from datetime import timedelta
from typing import Any

from async_durable_execution import durable_execution, map, wait


async def map_fn(item: str) -> str:
    return item.upper()


@durable_execution
async def handler(_event: Any) -> list:
    result = await map(map_fn, ["a", "b"], name="then-wait", max_concurrency=1)
    await wait(timedelta(seconds=1))
    return result.get_results()

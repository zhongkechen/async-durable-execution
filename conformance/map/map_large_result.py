"""9-16: Map with a large aggregate result."""

from typing import Any

from async_durable_execution import durable_execution, map


async def map_fn(_item: int) -> str:
    return "x" * 70000


@durable_execution
async def handler(_event: Any) -> dict:
    result = await map(map_fn, [0, 1, 2, 3], name="large", max_concurrency=1)
    return {
        "successCount": result.success_count,
        "totalCount": result.total_count,
    }

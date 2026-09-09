"""9-14: Map custom per-item serdes."""

from typing import Any

from async_durable_execution import SerDes, durable_execution, map


class WrapSerDes(SerDes[str | None]):
    async def serialize(self, value: str | None) -> str:
        return "null" if value is None else f"wrapped:{value}"

    async def deserialize(self, data: str) -> str | None:
        return None if data == "null" else data.removeprefix("wrapped:")


async def map_fn(item: str) -> str:
    return item.upper()


@durable_execution
async def handler(_event: Any) -> list:
    result = await map(
        map_fn,
        ["x", "y"],
        name="serdes",
        max_concurrency=1,
        item_serdes=WrapSerDes(),
    )
    return result.get_results()

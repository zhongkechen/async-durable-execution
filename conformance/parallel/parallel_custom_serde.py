"""8-15: Parallel custom per-branch serdes."""

import json
from typing import Any

from async_durable_execution import SerDes, durable_execution, parallel


class WrapSerDes(SerDes[str]):
    async def serialize(self, value: str) -> str:
        return json.dumps({"wrapped": value})

    async def deserialize(self, data: str) -> str:
        return json.loads(data)["wrapped"]


async def first() -> str:
    return "x"


async def second() -> str:
    return "y"


@durable_execution
async def handler(_event: Any) -> list:
    result = await parallel(
        [first, second],
        name="serde",
        max_concurrency=1,
        item_serdes=WrapSerDes(),
    )
    return result.get_results()

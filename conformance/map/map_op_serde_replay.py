"""9-20: Map operation-level serdes on replay."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
    JsonSerDes,
    SerDes,
    durable_execution,
    map,
    wait,
)


class OpSerDes(SerDes[BatchResult]):
    async def serialize(self, value: BatchResult) -> str:
        return "OPSERDE:" + ",".join(value.get_results())

    async def deserialize(self, payload: str) -> BatchResult:
        values = payload.removeprefix("OPSERDE:").split(",")
        items = [
            BatchItem(index=i, status=BatchItemStatus.SUCCEEDED, result=value)
            for i, value in enumerate(values)
        ]
        return BatchResult(items, CompletionReason.ALL_COMPLETED)


async def map_fn(item: str) -> str:
    return item.upper()


@durable_execution
async def handler(_event: Any) -> list:
    result = await map(
        map_fn,
        ["x", "y"],
        name="op-serde-replay",
        max_concurrency=1,
        serdes=OpSerDes(),
        item_serdes=JsonSerDes(),
    )
    await wait(timedelta(seconds=1))
    return result.get_results()

from async_durable_execution import SerDes, create_callback, durable_execution

# 4-16: Callback with custom serdes (numeric)
import json
from typing import Any


class NumericSerDes(SerDes[int]):
    async def serialize(self, value: int | None) -> str | None:
        if value is None:
            return None
        return json.dumps(value)

    async def deserialize(self, payload: str | None) -> int | None:
        if payload is None:
            return None
        return int(json.loads(payload))


@durable_execution
async def handler(event: Any) -> dict[str, Any]:
    callback = await create_callback(name=event, serdes=NumericSerDes())
    value = await callback.result()
    return {"count": value, "doubled": value * 2}

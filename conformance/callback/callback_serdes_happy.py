from async_durable_execution import SerDes, create_callback, durable_execution

# 4-15: Callback with custom serdes (happy path - Date roundtrip)
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class CustomData:
    id: int
    message: str
    timestamp: datetime


class CustomDataSerDes(SerDes[CustomData]):
    async def serialize(self, value: CustomData | None) -> str | None:
        if value is None:
            return None
        return json.dumps(
            {
                "id": value.id,
                "message": value.message,
                "timestamp": value.timestamp.isoformat(),
            }
        )

    async def deserialize(self, payload: str | None) -> CustomData | None:
        if payload is None:
            return None
        data = json.loads(payload)
        ts_str = data["timestamp"]
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        return CustomData(
            id=data["id"],
            message=data["message"],
            timestamp=datetime.fromisoformat(ts_str),
        )


@durable_execution
async def handler(event: Any) -> dict[str, Any]:
    callback = await create_callback(name=event, serdes=CustomDataSerDes())
    result: CustomData = await callback.result()
    return {
        "received": {
            "id": result.id,
            "message": result.message,
            "timestamp": int(result.timestamp.timestamp()),
        },
    }

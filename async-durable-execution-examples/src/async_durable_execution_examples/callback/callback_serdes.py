"""Demonstrates createCallback with custom serialization/deserialization for Date objects."""

import json
from datetime import datetime, timedelta
from typing import Any

from async_durable_execution import (
    durable_execution,
    SerDes,
    create_callback,
)


class CustomData:
    """Data structure with datetime."""

    def __init__(self, id: int, message: str, timestamp: datetime):
        self.id = id
        self.message = message
        self.timestamp = timestamp

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "CustomData":
        """Create from dictionary."""
        return CustomData(
            id=data["id"],
            message=data["message"],
            timestamp=datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")),
        )


class CustomDataSerDes(SerDes[CustomData]):
    """Custom serializer for CustomData that handles datetime conversion."""

    async def serialize(self, value: CustomData | None) -> str | None:
        """Serialize CustomData to JSON string."""
        if value is None:
            return None
        return json.dumps(value.to_dict())

    async def deserialize(self, payload: str | None) -> CustomData | None:
        """Deserialize JSON string to CustomData."""
        if payload is None:
            return None
        data = json.loads(payload)
        return CustomData.from_dict(data)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating createCallback with custom serdes."""
    callback = await create_callback(
        name="custom-serdes-callback",
        timeout=timedelta(seconds=30),
        serdes=CustomDataSerDes(),
    )

    result: CustomData = await callback.result()

    return {
        "receivedData": result.to_dict(),
        "isDateObject": isinstance(result.timestamp, datetime),
    }

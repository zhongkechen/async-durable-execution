"""Demonstrates createCallback with custom serialization/deserialization for Date objects."""

import json
from datetime import datetime, timedelta
from typing import Any

from async_durable_execution import (
    LambdaContext,
    CallbackConfig,
    durable_execution,
    SerDes,
    SerDesContext,
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

    def serialize(self, value: CustomData | None, _: SerDesContext) -> str | None:
        """Serialize CustomData to JSON string."""
        if value is None:
            return None
        return json.dumps(value.to_dict())

    def deserialize(self, payload: str | None, _: SerDesContext) -> CustomData | None:
        """Deserialize JSON string to CustomData."""
        if payload is None:
            return None
        data = json.loads(payload)
        return CustomData.from_dict(data)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> dict[str, Any]:
    """Handler demonstrating createCallback with custom serdes."""
    callback_config = CallbackConfig(
        timeout=timedelta(seconds=30),
        serdes=CustomDataSerDes(),
    )

    callback = await create_callback(
        name="custom-serdes-callback",
        config=callback_config,
    )

    result: CustomData = await callback.result()

    return {
        "receivedData": result.to_dict(),
        "isDateObject": isinstance(result.timestamp, datetime),
    }

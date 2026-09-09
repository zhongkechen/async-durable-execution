"""Demonstrates waitForCallback with custom serialization/deserialization."""

import json
from datetime import datetime, timedelta
from typing import Any, TypedDict

from async_durable_execution import (
    durable_callable,
    durable_execution,
    SerDes,
    wait_for_callback,
)


class CustomDataMetadata(TypedDict):
    """Metadata for CustomData."""

    version: str
    processed: bool


class CustomData(TypedDict):
    """Custom data structure with datetime."""

    id: int
    message: str
    timestamp: datetime
    metadata: CustomDataMetadata


class CustomSerdes(SerDes[CustomData]):
    """Custom serialization/deserialization for CustomData."""

    @staticmethod
    async def serialize(data: CustomData) -> str:
        """Serialize CustomData to JSON string."""
        if data is None:
            return None

        serialized_data = {
            "id": data["id"],
            "message": data["message"],
            "timestamp": data["timestamp"].isoformat(),
            "metadata": data["metadata"],
            "_serializedBy": "custom-serdes-v1",
        }
        return json.dumps(serialized_data)

    @staticmethod
    async def deserialize(data_str: str) -> CustomData:
        """Deserialize JSON string to CustomData."""
        if data_str is None:
            return None

        parsed = json.loads(data_str)
        return CustomData(
            id=parsed["id"],
            message=parsed["message"],
            timestamp=datetime.fromisoformat(
                parsed["timestamp"].replace("Z", "+00:00")
            ),
            metadata=CustomDataMetadata(
                version=parsed["metadata"]["version"],
                processed=parsed["metadata"]["processed"],
            ),
        )


@durable_callable
async def noop_submitter() -> None:
    return None


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with custom serdes."""

    result: CustomData = await wait_for_callback(
        noop_submitter(),
        name="custom-serdes-callback",
        timeout=timedelta(seconds=10),
        heartbeat_timeout=timedelta(seconds=20),
        serdes=CustomSerdes(),
    )

    isDateObject = isinstance(result["timestamp"], datetime)
    # Convert timestamp to ISO format because the Lambda result must remain JSON-serializable.
    result["timestamp"] = result["timestamp"].isoformat()

    return {
        "receivedData": result,
        "isDateObject": isDateObject,
    }

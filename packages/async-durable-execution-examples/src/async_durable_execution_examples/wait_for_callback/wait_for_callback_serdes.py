"""Demonstrates waitForCallback with custom serialization/deserialization."""

import json
from datetime import datetime, timedelta
from typing import Any, TypedDict

from async_durable_execution.config import WaitForCallbackConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution
from async_durable_execution.serdes import SerDes


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
    def serialize(data: CustomData, _=None) -> str:
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
    def deserialize(data_str: str, _=None) -> CustomData:
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


async def noop_submitter(_callback_id: str, _context: DurableContext) -> None:
    return None


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with custom serdes."""

    config = WaitForCallbackConfig(
        timeout=timedelta(seconds=10),
        heartbeat_timeout=timedelta(seconds=20),
        serdes=CustomSerdes(),
    )

    result: CustomData = await context.wait_for_callback(
        noop_submitter,
        name="custom-serdes-callback",
        config=config,
    )

    isDateObject = isinstance(result["timestamp"], datetime)
    # Convert timestamp to ISO format because the Lambda result must remain JSON-serializable.
    result["timestamp"] = result["timestamp"].isoformat()

    return {
        "receivedData": result,
        "isDateObject": isDateObject,
    }

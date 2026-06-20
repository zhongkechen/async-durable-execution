"""Demonstrates handler execution without any durable operations."""

import json
import time
from typing import Any

from async_durable_execution import LambdaContext, durable_execution


@durable_execution
async def handler(event: Any, context: LambdaContext) -> dict[str, Any]:
    """Handler that executes without any durable operations."""
    return {
        "received": json.dumps(event),
        "timestamp": int(time.time() * 1000),  # milliseconds since epoch
        "message": "Handler completed successfully",
    }

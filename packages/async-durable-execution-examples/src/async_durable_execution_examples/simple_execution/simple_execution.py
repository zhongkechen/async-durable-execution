"""Demonstrates handler execution without any durable operations."""

import json
import time
from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(event: Any, _context: DurableContext) -> dict[str, Any]:
    """Handler that executes without any durable operations."""
    return {
        "received": json.dumps(event),
        "timestamp": int(time.time() * 1000),  # milliseconds since epoch
        "message": "Handler completed successfully",
    }

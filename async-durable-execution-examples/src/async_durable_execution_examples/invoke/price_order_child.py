"""Child Lambda used by the invoke example."""

from typing import Any

from async_durable_execution import durable_execution


@durable_execution
async def handler(_event: dict[str, Any]) -> dict[str, Any]:
    return {
        "price": 42,
        "currency": "USD",
    }

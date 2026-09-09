"""Demonstrates how handler-level errors are captured and structured in results."""

from typing import Any

from async_durable_execution import (
    durable_execution,
)


@durable_execution
async def handler(_event: Any) -> None:
    """Handler demonstrating handler-level error capture."""
    # Simulate a handler-level error that might occur in real applications
    raise Exception("Intentional handler failure")

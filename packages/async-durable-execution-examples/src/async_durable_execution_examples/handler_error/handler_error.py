"""Demonstrates how handler-level errors are captured and structured in results."""

from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, _context: DurableContext) -> None:
    """Handler demonstrating handler-level error capture."""
    # Simulate a handler-level error that might occur in real applications
    raise Exception("Intentional handler failure")

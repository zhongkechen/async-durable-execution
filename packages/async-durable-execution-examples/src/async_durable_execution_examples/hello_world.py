"""Simple durable Lambda handler example.

This example demonstrates:
- Step execution with logging
- Wait operations (pausing without consuming resources)
- Replay-aware logging
- Returning a response
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    durable_execution,
    wait,
)

logger = logging.getLogger(__name__)


@durable_step
async def step_1() -> None:
    """First step that logs a message."""
    logger.info("Hello from step1")


@durable_step
async def step_2(status_code: int) -> str:
    """Second step that returns a message."""
    logger.info("Returning message with status code: %d", status_code)
    return f"Hello from Durable Lambda! (status: {status_code})"


@durable_execution
async def handler(event: Any) -> dict[str, Any]:
    """Durable Lambda handler with steps, waits, and logging.

    Args:
        event: Lambda event input
        context: Durable execution context

    Returns:
        Response dictionary with statusCode and body
    """
    # Execute Step #1 - logs a message
    await step(step_1())

    # Pause for 10 seconds without consuming CPU cycles or incurring usage charges
    # The execution will suspend here and resume after 10 seconds
    await wait(timedelta(seconds=10))

    logger.info("Waited for 10 seconds")

    # Execute Step #2 - returns a message with status code
    message = await step(step_2(status_code=200))

    # Return response
    return {
        "statusCode": 200,
        "body": message,
    }

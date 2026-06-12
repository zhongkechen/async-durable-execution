"""Demonstrates multiple invocations tracking with waitForCallback operations across different invocations."""

from datetime import timedelta
from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating multiple invocations with waitForCallback operations."""
    # First invocation - wait operation
    context.wait(timedelta(seconds=1), name="wait-invocation-1")

    # First callback operation
    async def first_submitter(callback_id: str, _context) -> None:
        """Submitter for first callback."""
        print(f"First callback submitted with ID: {callback_id}")

    callback_result_1: str = context.wait_for_callback(
        first_submitter,
        name="first-callback",
    )

    async def process_callback_data(_) -> dict[str, Any]:
        return {"processed": True, "step": 1}

    # Step operation between callbacks
    step_result: dict[str, Any] = context.step(
        process_callback_data,
        name="process-callback-data",
    )

    # Second invocation - another wait operation
    context.wait(timedelta(seconds=1), name="wait-invocation-2")

    # Second callback operation
    async def second_submitter(callback_id: str, _context) -> None:
        """Submitter for second callback."""
        print(f"Second callback submitted with ID: {callback_id}")

    callback_result_2: str = context.wait_for_callback(
        second_submitter,
        name="second-callback",
    )

    # Final invocation returns complete result
    return {
        "firstCallback": callback_result_1,
        "secondCallback": callback_result_2,
        "stepResult": step_result,
        "invocationCount": "multiple",
    }

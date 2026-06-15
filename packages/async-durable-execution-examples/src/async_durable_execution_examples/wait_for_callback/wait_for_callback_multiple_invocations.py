"""Demonstrates multiple invocations tracking with waitForCallback operations across different invocations."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    durable_execution,
    wait,
    wait_for_callback,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating multiple invocations with waitForCallback operations."""
    # First invocation - wait operation
    await wait(timedelta(seconds=1), name="wait-invocation-1")

    # First callback operation
    async def first_submitter(callback_id: str, _context) -> None:
        """Submitter for first callback."""
        print(f"First callback submitted with ID: {callback_id}")

    callback_result_1: str = await wait_for_callback(
        first_submitter,
        name="first-callback",
    )

    @durable_step
    async def process_callback_data() -> dict[str, Any]:
        return {"processed": True, "step": 1}

    # Step operation between callbacks
    step_result: dict[str, Any] = await step(
        process_callback_data(), name="process-callback-data"
    )

    # Second invocation - another wait operation
    await wait(timedelta(seconds=1), name="wait-invocation-2")

    # Second callback operation
    async def second_submitter(callback_id: str, _context) -> None:
        """Submitter for second callback."""
        print(f"Second callback submitted with ID: {callback_id}")

    callback_result_2: str = await wait_for_callback(
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

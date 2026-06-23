"""Demonstrates with_retry wrapping a wait_for_callback operation.

The callback may fail multiple times before succeeding. The with_retry helper
retries the entire callback flow (including creating a new callback each attempt)
with exponential backoff between attempts.
"""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    RetryStrategyBuilder,
    WaitForCallbackConfig,
    durable_execution,
    with_retry,
    wait_for_callback,
)
from async_durable_execution import WithRetryConfig


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating with_retry around a wait_for_callback.

    The external system may fail to process the callback multiple times.
    with_retry will re-create the callback and wait again on each retry,
    with exponential backoff between attempts.
    """

    async def retryable_callback_flow(attempt: int) -> str:
        """The retryable block: create a callback and wait for the result."""

        async def submitter(callback_id: str) -> None:
            """Submit the callback ID to an external system."""
            # In real usage, this would send the callback_id to an external
            # system (e.g., via API call, SQS message, etc.)

        config = WaitForCallbackConfig(
            timeout=timedelta(seconds=3),
            heartbeat_timeout=timedelta(seconds=3),
        )

        return await wait_for_callback(
            submitter, name=f"external-call-attempt-{attempt}", config=config
        )

    retry_config = WithRetryConfig(
        retry_strategy=RetryStrategyBuilder(
            max_attempts=5,
            initial_delay=timedelta(seconds=1),
            backoff_rate=1.0,
        ).build(),
    )

    result = await with_retry(
        retryable_callback_flow, retry_config, name="callback-with-retry"
    )

    return {
        "success": True,
        "result": result,
    }

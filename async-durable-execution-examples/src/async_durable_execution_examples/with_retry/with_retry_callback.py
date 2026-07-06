"""Demonstrates with_retry wrapping a wait_for_callback operation.

The callback may fail multiple times before succeeding. The with_retry helper
retries the entire callback flow (including creating a new callback each attempt)
with exponential backoff between attempts.
"""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    RetryStrategy,
    WithRetryContext,
    durable_callable,
    durable_execution,
    get_current_context,
    with_retry,
    wait_for_callback,
)


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating with_retry around a wait_for_callback.

    The external system may fail to process the callback multiple times.
    with_retry will re-create the callback and wait again on each retry,
    with exponential backoff between attempts.
    """

    async def retryable_callback_flow() -> str:
        """The retryable block: create a callback and wait for the result."""
        retry_context = get_current_context()
        assert isinstance(retry_context, WithRetryContext)

        @durable_callable
        async def submitter() -> None:
            """Submit the callback ID to an external system."""
            callback_id = get_current_context().callback_id
            del callback_id
            # In real usage, this would send the callback_id to an external
            # system (e.g., via API call, SQS message, etc.)

        return await wait_for_callback(
            submitter(),
            name=f"external-call-attempt-{retry_context.attempt}",
            timeout=timedelta(seconds=3),
            heartbeat_timeout=timedelta(seconds=3),
        )

    result = await with_retry(
        retryable_callback_flow,
        name="callback-with-retry",
        retry_strategy=RetryStrategy(
            max_attempts=5,
            initial_delay=timedelta(seconds=1),
            backoff_rate=1.0,
        ),
    )

    return {
        "success": True,
        "result": result,
    }

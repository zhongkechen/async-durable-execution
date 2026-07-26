"""Demonstrates waitForCallback with submitter retry strategy using exponential backoff (0.5s, 1s, 2s)."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    get_current_context,
    RetryStrategy,
    wait_for_callback,
)


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with submitter retry and exponential backoff."""

    @durable_callable
    async def submitter() -> None:
        """Submitter function that can fail based on event parameter."""
        callback_id = get_current_context().callback_id
        print(f"Submitting callback to external system - callbackId: {callback_id}")
        raise Exception("Simulated submitter failure")

    result: str = await wait_for_callback(
        submitter(),
        name="retry-submitter-callback",
        timeout=timedelta(seconds=3),
        heartbeat_timeout=timedelta(seconds=3),
        retry_strategy=RetryStrategy(
            max_attempts=3,
            initial_delay=timedelta(seconds=1),
            max_delay=timedelta(seconds=1),
        ),
    )

"""Demonstrates waitForCallback with submitter retry strategy using exponential backoff (0.5s, 1s, 2s)."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    LambdaContext,
    WaitForCallbackConfig,
    durable_execution,
    RetryStrategyBuilder,
    wait_for_callback,
)


@durable_execution
async def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with submitter retry and exponential backoff."""

    async def submitter(callback_id: str) -> None:
        """Submitter function that can fail based on event parameter."""
        print(f"Submitting callback to external system - callbackId: {callback_id}")
        raise Exception("Simulated submitter failure")

    config = WaitForCallbackConfig(
        timeout=timedelta(seconds=3),
        heartbeat_timeout=timedelta(seconds=3),
        retry_strategy=RetryStrategyBuilder(
            max_attempts=3,
            initial_delay=timedelta(seconds=1),
            max_delay=timedelta(seconds=1),
        ).build(),
    )

    result: str = await wait_for_callback(
        submitter,
        name="retry-submitter-callback",
        config=config,
    )

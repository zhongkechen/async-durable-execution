"""Demonstrates waitForCallback with submitter retry strategy using exponential backoff (0.5s, 1s, 2s)."""

from typing import Any

from async_durable_execution.config import Duration, WaitForCallbackConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution
from async_durable_execution.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


@durable_execution
async def handler(event: dict[str, Any], context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback with submitter retry and exponential backoff."""

    async def submitter(callback_id: str, _context) -> None:
        """Submitter function that can fail based on event parameter."""
        print(f"Submitting callback to external system - callbackId: {callback_id}")
        raise Exception("Simulated submitter failure")

    config = WaitForCallbackConfig(
        timeout=Duration.from_seconds(10),
        heartbeat_timeout=Duration.from_seconds(20),
        retry_strategy=create_retry_strategy(
            config=RetryStrategyConfig(
                max_attempts=3,
                initial_delay=Duration.from_seconds(1),
                max_delay=Duration.from_seconds(1),
            )
        ),
    )

    result: str = context.wait_for_callback(
        submitter,
        name="retry-submitter-callback",
        config=config,
    )

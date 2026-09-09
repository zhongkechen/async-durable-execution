"""MicroVM lifecycle managed by a durable terminal scope."""

from __future__ import annotations

from functools import partial
from typing import Any

from async_durable_execution import (
    DurableTerminalActions,
    RetryStrategy,
    create_callback,
    durable_callable,
    durable_execution,
    step,
    terminal_scope,
)


@durable_callable
async def launch_microvm(review_id: str) -> dict[str, str]:
    """Launch the isolated environment used for one review."""
    return {
        "microvm_id": f"microvm-{review_id}",
        "review_id": review_id,
    }


@durable_callable
async def terminate_microvm(_microvm_id: str) -> None:
    """Release the MicroVM after any logical terminal outcome."""


@durable_callable
async def cancel_review(_microvm_id: str) -> None:
    """Compensate a review that failed after the MicroVM was acquired."""


@durable_callable
async def dispatch_review(
    _microvm: dict[str, str],
    _callback_id: str,
) -> None:
    """Submit review work to an external system."""


@durable_callable
async def fail_review() -> None:
    """Provide a deterministic failure path for the cloud example test."""
    msg = "Review dispatch failed"
    raise RuntimeError(msg)


async def review_with_microvm(
    event: dict[str, Any],
    terminal: DurableTerminalActions,
) -> dict[str, Any]:
    """Run a callback-based review and register its terminal actions."""
    review_id = str(event.get("review_id", "example"))
    microvm = await step(
        launch_microvm(review_id),
        name="launch-microvm",
    )

    terminal.cleanup(
        terminate_microvm(microvm["microvm_id"]),
        name="terminate-microvm",
    )
    terminal.compensate(
        cancel_review(microvm["microvm_id"]),
        name="cancel-review",
    )

    if event.get("fail"):
        await step(
            fail_review(),
            name="fail-review",
            retry_strategy=RetryStrategy.none(),
        )

    callback = await create_callback(name="review-complete")
    await step(
        dispatch_review(microvm, callback.callback_id),
        name="dispatch-review",
    )
    review_result = await callback.result()
    return {
        "microvm_id": microvm["microvm_id"],
        "review": review_result,
    }


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Run the MicroVM review in a suspension-aware terminal scope."""
    return await terminal_scope(
        partial(review_with_microvm, event),
        name="review-with-microvm",
    )

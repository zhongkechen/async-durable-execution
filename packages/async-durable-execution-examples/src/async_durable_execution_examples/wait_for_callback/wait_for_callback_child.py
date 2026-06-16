"""Demonstrates waitForCallback operations within child contexts."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_execution,
    run_in_child_context,
    wait,
    wait_for_callback,
)


async def noop_submitter(_callback_id: str) -> None:
    return None


async def child_context_with_callback() -> dict[str, Any]:
    """Child context containing wait and callback operations."""
    await wait(timedelta(seconds=1), name="child-wait")

    child_callback_result: str = await wait_for_callback(
        noop_submitter, name="child-callback-op"
    )

    return {
        "childResult": child_callback_result,
        "childProcessed": True,
    }


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating waitForCallback within child contexts."""
    parent_result: str = await wait_for_callback(
        noop_submitter, name="parent-callback-op"
    )

    child_context_result: dict[str, Any] = await run_in_child_context(
        child_context_with_callback,
        name="child-context-with-callback",
    )

    return {
        "parentResult": parent_result,
        "childContextResult": child_context_result,
    }

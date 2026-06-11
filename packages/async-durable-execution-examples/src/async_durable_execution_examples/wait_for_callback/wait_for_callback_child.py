"""Demonstrates waitForCallback operations within child contexts."""

from typing import Any

from async_durable_execution.config import Duration
from async_durable_execution.context import (
    DurableContext,
    durable_with_child_context,
)
from async_durable_execution.execution import durable_execution


async def noop_submitter(_callback_id: str, _context: DurableContext) -> None:
    return None


@durable_with_child_context
async def child_context_with_callback(child_context: DurableContext) -> dict[str, Any]:
    """Child context containing wait and callback operations."""
    child_context.wait(Duration.from_seconds(1), name="child-wait")

    child_callback_result: str = child_context.wait_for_callback(
        noop_submitter, name="child-callback-op"
    )

    return {
        "childResult": child_callback_result,
        "childProcessed": True,
    }


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Handler demonstrating waitForCallback within child contexts."""
    parent_result: str = context.wait_for_callback(
        noop_submitter, name="parent-callback-op"
    )

    child_context_result: dict[str, Any] = context.run_in_child_context(
        child_context_with_callback(), name="child-context-with-callback"
    )

    return {
        "parentResult": parent_result,
        "childContextResult": child_context_result,
    }

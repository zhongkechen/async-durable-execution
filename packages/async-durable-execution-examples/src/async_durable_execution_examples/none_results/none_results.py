"""Demonstrates handling of operations that return undefined values during replay."""

from datetime import timedelta
from typing import Any

from async_durable_execution.context import (
    DurableContext,
    durable_with_child_context,
)
from async_durable_execution.execution import durable_execution


@durable_with_child_context
async def parent_context(ctx: DurableContext) -> None:
    """Parent context that returns None."""
    return None


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    """Handler demonstrating operations with undefined/None results."""

    async def fetch_user(_) -> None:
        return None

    context.step(fetch_user, name="fetch-user")

    context.run_in_child_context(parent_context(), name="parent")

    context.wait(timedelta(seconds=1), name="wait")

    return "result"

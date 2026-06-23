"""Demonstrates handling of operations that return undefined values during replay."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_callable,
    durable_execution,
    durable_callable,
    run_in_child_context,
    step,
    wait,
)


@durable_callable
async def parent_context() -> None:
    """Parent context that returns None."""
    return None


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> str:
    """Handler demonstrating operations with undefined/None results."""

    @durable_callable
    async def fetch_user() -> None:
        return None

    await step(fetch_user(), name="fetch-user")

    await run_in_child_context(parent_context(), name="parent")

    await wait(timedelta(seconds=1), name="wait")

    return "result"

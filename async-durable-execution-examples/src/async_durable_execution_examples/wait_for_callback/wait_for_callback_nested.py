"""Demonstrates nested waitForCallback operations across multiple child context levels."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    run_in_child_context,
    wait,
    wait_for_callback,
)


async def noop_submitter(_callback_id: str) -> None:
    return None


@durable_callable
async def inner_child_context() -> dict[str, Any]:
    """Inner child context with deep nested callback."""
    await wait(timedelta(seconds=1), name="deep-wait")

    nested_callback_result: str = await wait_for_callback(
        noop_submitter,
        name="nested-callback-op",
    )

    return {
        "nestedCallback": nested_callback_result,
        "deepLevel": "inner-child",
    }


@durable_callable
async def outer_child_context() -> dict[str, Any]:
    """Outer child context with inner callback and nested context."""
    inner_result: str = await wait_for_callback(
        noop_submitter,
        name="inner-callback-op",
    )

    # Nested child context with another callback
    deep_nested_result: dict[str, Any] = await run_in_child_context(
        inner_child_context(),
        name="inner-child-context",
    )

    return {
        "innerCallback": inner_result,
        "deepNested": deep_nested_result,
        "level": "outer-child",
    }


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating nested waitForCallback operations across multiple levels."""
    outer_result: str = await wait_for_callback(
        noop_submitter,
        name="outer-callback-op",
    )

    nested_result: dict[str, Any] = await run_in_child_context(
        outer_child_context(),
        name="outer-child-context",
    )

    return {
        "outerCallback": outer_result,
        "nestedResults": nested_result,
    }

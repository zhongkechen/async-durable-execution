"""Example demonstrating nested child contexts (blocks)."""

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_execution,
    durable_step,
    run_in_child_context,
    step,
    wait,
)


async def nested_block() -> str:
    """Nested block with its own child context."""
    # Wait in the nested block
    await wait(timedelta(seconds=1))
    return "nested block result"


async def parent_block() -> dict[str, str]:
    """Parent block with nested operations."""

    @durable_step
    async def build_nested_result() -> str:
        return "nested step result"

    # Nested step
    nested_result: str = await step(
        build_nested_result(),
        name="nested_step",
    )

    # Nested block with its own child context
    nested_block_result: str = await run_in_child_context(
        nested_block, name="nested_block"
    )

    return {
        "nestedStep": nested_result,
        "nestedBlock": nested_block_result,
    }


@durable_execution
async def handler(_event: Any) -> dict[str, str]:
    """Handler demonstrating nested child contexts."""
    # Run parent block which contains nested operations
    result: dict[str, str] = await run_in_child_context(
        parent_block,
        name="parent_block",
    )

    return result

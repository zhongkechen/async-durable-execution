"""Example demonstrating nested child contexts (blocks)."""

from typing import Any

from async_durable_execution.context import (
    DurableContext,
    durable_with_child_context,
)
from async_durable_execution.execution import durable_execution
from async_durable_execution.config import Duration


@durable_with_child_context
async def nested_block(ctx: DurableContext) -> str:
    """Nested block with its own child context."""
    # Wait in the nested block
    ctx.wait(Duration.from_seconds(1))
    return "nested block result"


@durable_with_child_context
async def parent_block(ctx: DurableContext) -> dict[str, str]:
    """Parent block with nested operations."""

    async def build_nested_result(_) -> str:
        return "nested step result"

    # Nested step
    nested_result: str = ctx.step(
        build_nested_result,
        name="nested_step",
    )

    # Nested block with its own child context
    nested_block_result: str = ctx.run_in_child_context(nested_block())

    return {
        "nestedStep": nested_result,
        "nestedBlock": nested_block_result,
    }


@durable_execution
async def handler(_event: Any, context: DurableContext) -> dict[str, str]:
    """Handler demonstrating nested child contexts."""
    # Run parent block which contains nested operations
    result: dict[str, str] = context.run_in_child_context(
        parent_block(), name="parent_block"
    )

    return result

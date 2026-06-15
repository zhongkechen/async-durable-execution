"""Complex multi-operation example demonstrating all major operations."""

from datetime import timedelta
from typing import Any

from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


@durable_execution
async def handler(event: dict[str, Any], context: DurableContext) -> dict[str, Any]:
    """Comprehensive example demonstrating all major durable operations."""
    print(f"Starting comprehensive operations example with event: {event}")

    async def run_step_one() -> str:
        return "Step 1 completed successfully"

    async def map_item(ctx: DurableContext, item: int, index: int, _) -> int:
        async def get_item() -> int:
            return item

        return await ctx.step(get_item, name=f"map-step-{index}")

    async def fruit_step_1(ctx: DurableContext) -> str:
        async def get_fruit() -> str:
            return "apple"

        return await ctx.step(get_fruit, name="fruit-step-1")

    async def fruit_step_2(ctx: DurableContext) -> str:
        async def get_fruit() -> str:
            return "banana"

        return await ctx.step(get_fruit, name="fruit-step-2")

    async def fruit_step_3(ctx: DurableContext) -> str:
        async def get_fruit() -> str:
            return "orange"

        return await ctx.step(get_fruit, name="fruit-step-3")

    # Step 1: ctx.step - Simple step that returns a result
    step1_result: str = await context.step(
        run_step_one,
        name="step1",
    )

    # Step 2: ctx.wait - Wait for 1 second
    await context.wait(timedelta(seconds=1))

    # Step 3: ctx.map - Map with 5 iterations returning numbers 1 to 5
    map_input = [1, 2, 3, 4, 5]

    map_results = (
        await context.map(
            inputs=map_input,
            func=map_item,
            name="map-numbers",
        )
    ).to_dict()

    # Step 4: ctx.parallel - 3 branches, each returning a fruit name

    parallel_results = (
        await context.parallel(functions=[fruit_step_1, fruit_step_2, fruit_step_3])
    ).to_dict()

    # Final result combining all operations
    return {
        "step1": step1_result,
        "waitCompleted": True,
        "mapResults": map_results,
        "parallelResults": parallel_results,
    }

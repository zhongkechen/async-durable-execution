"""Example demonstrating logger usage in durable contexts."""

import logging
from typing import Any

from async_durable_execution import (
    durable_step,
    step,
    durable_execution,
    run_in_child_context,
)

logger = logging.getLogger(__name__)


async def child_workflow() -> str:
    """Child workflow with its own logging context."""
    logger.info("Running in child context")

    # Step in child context has nested step ID
    @durable_step
    async def child_step() -> str:
        return "child-processed"

    child_result: str = await step(child_step(), name="child_step")

    logger.info("Child workflow completed", extra={"result": child_result})

    return child_result


@durable_step
async def my_step(my_arg: int) -> str:
    logger.info("Hello from my_step")
    logger.warning("Warning from my_step", extra={"my_arg": my_arg})
    logger.error("Error from my_step", extra={"my_arg": my_arg, "type": "error"})
    return f"from my_step: {my_arg}"


@durable_execution
async def handler(event: Any) -> str:
    """Handler demonstrating logger usage."""
    logger.info("Starting workflow", extra={"eventId": event.get("id")})

    # Logger in steps - gets enriched with step ID and attempt number
    @durable_step
    async def process_data() -> str:
        return "processed"

    result1: str = await step(process_data(), name="process_data")

    await step(my_step(123))

    logger.info("Step 1 completed", extra={"result": result1})

    # Child contexts inherit the parent's logger and have their own step ID
    result2: str = await run_in_child_context(
        child_workflow,
        name="child_workflow",
    )

    logger.info("Workflow completed", extra={"result1": result1, "result2": result2})

    return f"{result1}-{result2}"

"""Demonstrates handler execution without any durable operations."""

import logging
from typing import Any

from async_durable_execution import StepContext
from async_durable_execution.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from async_durable_execution.execution import durable_execution
from async_durable_execution.plugin import (
    DurableInstrumentationPlugin,
)


class MyPlugin(DurableInstrumentationPlugin):
    logger = logging.getLogger("MyPlugin")

    def on_operation_start(self, info):
        self.logger.info("Operation started: %s", info)

    def on_operation_end(self, info):
        self.logger.info("Operation ended: %s", info)

    def on_invocation_start(self, info):
        self.logger.info("Invocation started: %s", info)

    def on_invocation_end(self, info):
        self.logger.info("Invocation ended: %s", info)

    def on_user_function_start(self, info) -> None:
        self.logger.info("User function started: %s", info)

    def on_user_function_end(self, info) -> None:
        self.logger.info("User function ended: %s", info)


@durable_step
async def add_numbers(_step_context: StepContext, a: int, b: int) -> int:
    return a + b


@durable_with_child_context
async def add_numbers_in_child(child_context: DurableContext, a: int, b: int):
    result: int = await child_context.step(
        add_numbers(a, b),
        name="add-a-and-b",
    )
    return result


@durable_execution(plugins=[MyPlugin()])
async def handler(_event: Any, context: DurableContext) -> int:
    result: int = await context.run_in_child_context(
        add_numbers_in_child(6, 4),
        name="add-6-and-4",
    )
    return await context.step(
        add_numbers(result, 2),
        name="add-result-to-2",
    )

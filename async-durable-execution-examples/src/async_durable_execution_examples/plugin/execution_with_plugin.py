"""Demonstrates handler execution without any durable operations."""

import logging
from typing import Any

from async_durable_execution import (
    LambdaContext,
    DurableInstrumentationPlugin,
    durable_callable,
    durable_execution,
    durable_callable,
    run_in_child_context,
    step,
)


class MyPlugin(DurableInstrumentationPlugin):
    logger = logging.getLogger("MyPlugin")

    async def on_operation_start(self, info):
        self.logger.info("Operation started: %s", info)

    async def on_operation_end(self, info):
        self.logger.info("Operation ended: %s", info)

    async def on_invocation_start(self, info):
        self.logger.info("Invocation started: %s", info)

    async def on_invocation_end(self, info):
        self.logger.info("Invocation ended: %s", info)

    async def on_user_function_start(self, info) -> None:
        self.logger.info("User function started: %s", info)

    async def on_user_function_end(self, info) -> None:
        self.logger.info("User function ended: %s", info)


@durable_callable
async def add_numbers(a: int, b: int) -> int:
    return a + b


@durable_callable
async def add_numbers_in_child(a: int, b: int):
    result: int = await step(
        add_numbers(a, b),
        name="add-a-and-b",
    )
    return result


@durable_execution(plugins=[MyPlugin()])
async def handler(_event: Any, context: LambdaContext) -> int:
    result: int = await run_in_child_context(
        add_numbers_in_child(6, 4),
        name="add-6-and-4",
    )
    return await step(
        add_numbers(result, 2),
        name="add-result-to-2",
    )

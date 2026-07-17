"""1-7: Step handler that uses the SDK's step context logger.

Validates that the logger provided by StepContext correctly emits log
entries to CloudWatch during step execution.
"""

import logging
from async_durable_execution import durable_callable, durable_execution, step
from typing import Any

logger = logging.getLogger(__name__)


@durable_callable
async def greet(name: str) -> str:
    logger.info(f"Greeting step started for: {name}")
    result = f"Hello, {name}!"
    logger.info(f"Greeting step completed with: {result}")
    return result


@durable_execution
async def handler(event: Any) -> str:
    result: str = await step(greet(event))
    return result

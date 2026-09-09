"""1-9: Replay skips succeeded step - step logs once, proving no re-execution on replay."""

import logging
from datetime import timedelta
from async_durable_execution import durable_callable, durable_execution, step, wait
from typing import Any

logger = logging.getLogger(__name__)


@durable_callable
async def compute_with_log() -> str:
    logger.info("step executed")
    return "cached_value"


@durable_execution
async def handler(_event: Any) -> str:
    result: str = await step(compute_with_log())
    await wait(timedelta(seconds=1))
    return result

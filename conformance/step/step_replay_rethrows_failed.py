"""1-10: Replay re-throws failed step - step logs once, proving no re-execution on replay."""

import logging
from datetime import timedelta
from async_durable_execution import (
    RetryPresets,
    durable_callable,
    durable_execution,
    step,
    wait,
)
from typing import Any

logger = logging.getLogger(__name__)


@durable_callable
async def failing_with_log() -> str:
    logger.info("step executed")
    msg = "Something went wrong"
    raise RuntimeError(msg)


@durable_execution
async def handler(_event: Any) -> str:
    try:
        await step(failing_with_log(), retry_strategy=RetryPresets.none())
    except Exception as e:
        error_msg = str(e)

    await wait(timedelta(seconds=1))
    return f"caught: {error_msg}"

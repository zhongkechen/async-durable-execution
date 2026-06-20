from datetime import timedelta
from typing import Any

from async_durable_execution import (
    LambdaContext,
    durable_execution,
    wait,
)


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> str:
    # Wait with explicit name
    await wait(timedelta(seconds=1), name="custom_wait")
    return "Wait with name completed"

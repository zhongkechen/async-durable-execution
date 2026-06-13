from datetime import timedelta
from typing import TYPE_CHECKING, Any

from async_durable_execution.config import CallbackConfig
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


if TYPE_CHECKING:
    from async_durable_execution.types import Callback


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    callback_config = CallbackConfig(
        timeout=timedelta(seconds=120), heartbeat_timeout=timedelta(seconds=60)
    )

    callback: Callback[str] = await context.create_callback(
        name="example_callback", config=callback_config
    )

    return await callback.result()

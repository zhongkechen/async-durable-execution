from typing import TYPE_CHECKING, Any

from async_durable_execution.config import CallbackConfig, Duration
from async_durable_execution.context import DurableContext
from async_durable_execution.execution import durable_execution


if TYPE_CHECKING:
    from async_durable_execution.types import Callback


@durable_execution
async def handler(_event: Any, context: DurableContext) -> str:
    # Callback with custom timeout configuration
    config = CallbackConfig(
        timeout=Duration.from_seconds(60), heartbeat_timeout=Duration.from_seconds(30)
    )

    callback: Callback[str] = context.create_callback(
        name="timeout_callback", config=config
    )

    return f"Callback created with 60s timeout: {callback.callback_id}"

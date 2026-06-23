from datetime import timedelta
from typing import TYPE_CHECKING, Any

from async_durable_execution import (
    CallbackConfig,
    durable_execution,
    create_callback,
)

if TYPE_CHECKING:
    from async_durable_execution import Callback


@durable_execution
async def handler(_event: Any) -> str:
    # Callback with custom timeout configuration
    config = CallbackConfig(
        timeout=timedelta(seconds=60), heartbeat_timeout=timedelta(seconds=30)
    )

    callback: Callback[str] = await create_callback(
        name="timeout_callback", config=config
    )

    return f"Callback created with 60s timeout: {callback.callback_id}"

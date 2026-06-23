from datetime import timedelta
from typing import TYPE_CHECKING, Any

from async_durable_execution import (
    LambdaContext,
    CallbackConfig,
    durable_execution,
    create_callback,
)

if TYPE_CHECKING:
    from async_durable_execution import Callback


@durable_execution
async def handler(_event: Any, context: LambdaContext) -> str:
    callback_config = CallbackConfig(
        timeout=timedelta(seconds=120), heartbeat_timeout=timedelta(seconds=60)
    )

    callback: Callback[str] = await create_callback(
        name="example_callback", config=callback_config
    )

    return await callback.result()

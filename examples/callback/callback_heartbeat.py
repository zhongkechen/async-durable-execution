from datetime import timedelta
from typing import TYPE_CHECKING, Any

from async_durable_execution import (
    durable_execution,
    create_callback,
)

if TYPE_CHECKING:
    from async_durable_execution import Callback


@durable_execution
async def handler(_event: Any) -> str:
    callback: Callback[str] = await create_callback(
        name="heartbeat_callback",
        timeout=timedelta(seconds=60),
        heartbeat_timeout=timedelta(seconds=10),
    )

    return await callback.result()

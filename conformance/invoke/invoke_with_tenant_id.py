"""5-8: Invoke with tenantId (tenant-isolated invocation)."""

from async_durable_execution import durable_execution, invoke
import os
from typing import Any


@durable_execution
async def handler(event: Any) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    tenant_id = event["tenantId"]
    payload = event["payload"]
    result: str = await invoke(function_name, payload, tenant_id=tenant_id)
    return result

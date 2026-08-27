"""Cloud example that forces the sync Lambda API client."""

from typing import Any

from async_durable_execution import (
    create_default_sync_client,
    durable_callable,
    durable_execution,
    step,
)


_sync_lambda_client = create_default_sync_client()


@durable_callable
async def format_message(name: str) -> str:
    return f"hello {name}"


@durable_execution(boto3_client=_sync_lambda_client)
async def handler(event: dict[str, Any]) -> dict[str, str]:
    """Run on Lambda with the sync botocore client even when HTTPX is installed."""
    name = str(event.get("name", "cloud"))
    message = await step(format_message(name), name="format-message")
    return {"client": "sync", "message": message}

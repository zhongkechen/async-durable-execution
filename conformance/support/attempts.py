"""External attempt tracking for tests that deliberately terminate Lambda."""

import asyncio
import hashlib
import os

import boto3


ssm_client = boto3.client("ssm")
PARAMETER_PREFIX = os.environ.get(
    "ATTEMPTS_PARAMETER_PREFIX",
    "/durable-execution-conformance/attempts",
)


def _parameter_name(execution_id: str) -> str:
    execution_hash = hashlib.sha256(execution_id.encode()).hexdigest()
    return f"{PARAMETER_PREFIX}/{execution_hash}"


async def increment_attempt(execution_id: str) -> int:
    """Increment and return the attempt count for one durable execution."""
    parameter_name = _parameter_name(execution_id)
    try:
        response = await asyncio.to_thread(
            ssm_client.get_parameter,
            Name=parameter_name,
        )
        attempt = int(response["Parameter"]["Value"]) + 1
    except ssm_client.exceptions.ParameterNotFound:
        attempt = 1

    await asyncio.to_thread(
        ssm_client.put_parameter,
        Name=parameter_name,
        Value=str(attempt),
        Type="String",
        Overwrite=True,
    )
    return attempt

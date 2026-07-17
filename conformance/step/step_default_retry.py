"""1-13: Default retry strategy (uses DynamoDB to track attempts)."""

import asyncio
from async_durable_execution import (
    durable_callable,
    durable_execution,
    get_current_context,
    step,
)
import os
from typing import Any

import boto3

ddb_client = boto3.client("dynamodb")
TABLE_NAME = os.environ.get("ATTEMPTS_TABLE_NAME", "Attempts")


@durable_callable
async def unreliable(*, execution_id: str) -> str:
    # Atomically increment attempt counter in DynamoDB
    response = await asyncio.to_thread(
        ddb_client.update_item,
        TableName=TABLE_NAME,
        Key={"executionId": {"S": execution_id}},
        UpdateExpression="SET attemptCount = if_not_exists(attemptCount, :zero) + :inc",
        ExpressionAttributeValues={
            ":zero": {"N": "0"},
            ":inc": {"N": "1"},
        },
        ReturnValues="UPDATED_NEW",
    )

    attempt_count = int(response["Attributes"]["attemptCount"]["N"])

    if attempt_count < 3:
        msg = f"Attempt {attempt_count} failed"
        raise RuntimeError(msg)
    return "recovered"


@durable_execution
async def handler(_event: Any) -> str:
    execution_id = get_current_context().durable_execution_arn

    # Step with no explicit retry config — uses SDK default
    result: str = await step(unreliable(execution_id=execution_id))
    return result

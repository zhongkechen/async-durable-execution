"""1-15: Retry specific exception (uses DynamoDB to track attempts)."""

import asyncio
from datetime import timedelta
from async_durable_execution import (
    RetryStrategyBuilder,
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


class TransientError(Exception):
    """Custom transient error that should be retried."""


@durable_callable
async def transient_on_first(*, execution_id: str) -> str:
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

    if attempt_count < 2:
        raise TransientError("Temporary failure")
    return "recovered from transient"


@durable_execution
async def handler(_event: Any) -> str:
    execution_id = get_current_context().durable_execution_arn

    retry_strategy = RetryStrategyBuilder(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        retryable_error_types=[TransientError],
    ).build()

    result: str = await step(
        transient_on_first(execution_id=execution_id), retry_strategy=retry_strategy
    )
    return result

"""1-14: Retry with custom config (uses DynamoDB to track attempts)."""

import asyncio
from datetime import timedelta
from async_durable_execution import (
    JitterStrategy,
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


@durable_callable
async def flaky(*, execution_id: str) -> str:
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
    return "finally succeeded"


@durable_execution
async def handler(_event: Any) -> str:
    execution_id = get_current_context().durable_execution_arn

    retry_strategy = RetryStrategyBuilder(
        max_attempts=5,
        initial_delay=timedelta(seconds=2),
        backoff_rate=3,
        jitter_strategy=JitterStrategy.NONE,
    ).build()

    result: str = await step(
        flaky(execution_id=execution_id), retry_strategy=retry_strategy
    )
    return result

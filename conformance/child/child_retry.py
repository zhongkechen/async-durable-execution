"""3-7: Child context with step retry (fails then succeeds)."""

import asyncio
from async_durable_execution import (
    RetryStrategyBuilder,
    durable_callable,
    durable_execution,
    get_current_context,
    run_in_child_context,
    step,
)
import os
from typing import Any

import boto3

ddb_client = boto3.client("dynamodb")
TABLE_NAME = os.environ.get("ATTEMPTS_TABLE_NAME", "Attempts")


@durable_callable
async def unreliable_step(*, execution_id: str, value: str) -> str:
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
        msg = f"Attempt {attempt_count} failed"
        raise RuntimeError(msg)
    return value


@durable_callable
async def retry_child(*, execution_id: str, value: str) -> str:
    retry_strategy = RetryStrategyBuilder(
        max_attempts=3, retryable_error_types=[RuntimeError]
    ).build()

    return await step(
        unreliable_step(execution_id=execution_id, value=value),
        retry_strategy=retry_strategy,
    )


@durable_execution
async def handler(event: Any) -> str:
    execution_id = get_current_context().durable_execution_arn

    result: str = await run_in_child_context(
        retry_child(execution_id=execution_id, value=str(event)), name="retry-child"
    )
    return result

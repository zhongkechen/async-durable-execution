"""1-18: AtMostOnce interrupted (with retry, uses DynamoDB to track attempts)."""

import asyncio
from datetime import timedelta
from async_durable_execution import (
    RetryStrategyBuilder,
    StepSemantics,
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
async def at_most_once_step(*, execution_id: str, input_1: str) -> str:
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

    # Print input to stdout each time step executes
    print(input_1, flush=True)
    await asyncio.sleep(1)  # Allow time for logs to flush to CloudWatch

    if attempt_count < 2:
        # First attempt: simulate Lambda crash
        os._exit(1)
    # Second attempt (retry): succeed
    return "succeeded on second attempt"


@durable_execution
async def handler(event: Any) -> str:
    execution_id = get_current_context().durable_execution_arn

    retry_strategy = RetryStrategyBuilder(
        max_attempts=3, initial_delay=timedelta(seconds=1)
    ).build()

    result: str = await step(
        at_most_once_step(execution_id=execution_id, input_1=str(event)),
        retry_strategy=retry_strategy,
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
    )
    return result

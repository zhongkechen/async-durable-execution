"""Tests for wait_for_callback_multiple_invocations."""

import json

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.wait_for_callback import (
    wait_for_callback_multiple_invocations,
)


async def test_handle_multiple_invocations_tracking_with_wait_for_callback_operations(
    durable_runner,
):
    """Test multiple invocations tracking with waitForCallback operations."""
    test_payload = {"test": "multiple-invocations"}

    async with durable_runner(
        handler=wait_for_callback_multiple_invocations.handler,
        input=test_payload,
        timeout=60,
    ) as runner:
        # Start the execution (this will pause at callbacks)
        execution_arn = await runner.run_async()

        # Wait for first callback and get callback_id
        first_callback_id = await runner.wait_for_callback(execution_arn=execution_arn)

        # Complete first callback
        first_callback_result = json.dumps({"step": 1})
        await runner.send_callback_success(
            callback_id=first_callback_id, result=first_callback_result.encode()
        )

        # Wait for second callback and get callback_id
        second_callback_id = await runner.wait_for_callback(
            execution_arn=execution_arn, name="second-callback-callback"
        )

        # Complete second callback
        second_callback_result = json.dumps({"step": 2})
        await runner.send_callback_success(
            callback_id=second_callback_id, result=second_callback_result.encode()
        )

        # Wait for the execution to complete
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    assert result_data == {
        "firstCallback": '{"step": 1}',
        "secondCallback": '{"step": 2}',
        "stepResult": {"processed": True, "step": 1},
        "invocationCount": "multiple",
    }

    # Verify invocations were tracked - should be exactly 5 invocations
    # Note: Check if Python SDK provides invocations tracking
    if hasattr(result, "invocations"):
        invocations = result.invocations
        assert len(invocations) == 5

    # Verify operations were executed
    operations = result.operations
    assert len(operations) > 4  # wait + callback + step + wait + callback operations

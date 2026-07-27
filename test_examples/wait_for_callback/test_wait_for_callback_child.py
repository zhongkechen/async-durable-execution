"""Tests for wait_for_callback_child_context."""

import json

from async_durable_execution import InvocationStatus
from examples.wait_for_callback import wait_for_callback_child


async def test_handle_wait_for_callback_within_child_contexts(durable_runner) -> None:
    """Test waitForCallback within child contexts."""
    test_payload = {"test": "child-context-callbacks"}

    async with durable_runner(
        handler=wait_for_callback_child.handler, input=test_payload, timeout=30
    ) as runner:
        execution_arn = await runner.run_async()
        # Wait for parent callback and get callback_id
        parent_callback_id = await runner.wait_for_callback(execution_arn=execution_arn)
        # Send parent callback result
        parent_callback_result = json.dumps({"parentData": "parent-completed"})
        await runner.send_callback_success(
            callback_id=parent_callback_id, result=parent_callback_result.encode()
        )
        # Wait for child callback and get callback_id
        child_callback_id = await runner.wait_for_callback(
            execution_arn=execution_arn, name="child-callback-op-callback"
        )
        # Send child callback result
        child_callback_result = json.dumps({"childData": 42})
        await runner.send_callback_success(
            callback_id=child_callback_id, result=child_callback_result.encode()
        )
        # Wait for the execution to complete
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED
    result_data = result.get_deserialized_result()
    assert result_data == {
        "parentResult": parent_callback_result,
        "childContextResult": {
            "childResult": child_callback_result,
            "childProcessed": True,
        },
    }

    # Find the child context operation
    child_context_op = result.get_context("child-context-with-callback")

    # Verify child operations are accessible
    child_operations = result.get_child_operations(child_context_op)
    assert child_operations is not None
    assert len(child_operations) == 2  # wait + waitForCallback

    all_ops = result.get_all_operations()

    # Verify completed operations count
    completed_operations = [op for op in all_ops if op.status.value == "SUCCEEDED"]
    assert len(completed_operations) == 8

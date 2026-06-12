"""Tests for wait_for_callback_child_context."""

import json

from async_durable_execution.execution import InvocationStatus
from async_durable_execution_examples.wait_for_callback import wait_for_callback_child


def test_handle_wait_for_callback_within_child_contexts(durable_runner):
    """Test waitForCallback within child contexts."""
    test_payload = {"test": "child-context-callbacks"}

    with durable_runner(
        handler=wait_for_callback_child.handler, input=test_payload, timeout=30
    ) as runner:
        execution_arn = runner.run_async()
        # Wait for parent callback and get callback_id
        parent_callback_id = runner.wait_for_callback(execution_arn=execution_arn)
        # Send parent callback result
        parent_callback_result = json.dumps({"parentData": "parent-completed"})
        runner.send_callback_success(
            callback_id=parent_callback_id, result=parent_callback_result.encode()
        )
        # Wait for child callback and get callback_id
        child_callback_id = runner.wait_for_callback(
            execution_arn=execution_arn, name="child-callback-op create callback id"
        )
        # Send child callback result
        child_callback_result = json.dumps({"childData": 42})
        runner.send_callback_success(
            callback_id=child_callback_id, result=child_callback_result.encode()
        )
        # Wait for the execution to complete
        result = runner.wait_for_result(execution_arn=execution_arn)

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
    child_context_ops = [
        op
        for op in result.operations
        if op.operation_type.value == "CONTEXT"
        and op.name == "child-context-with-callback"
    ]
    assert len(child_context_ops) == 1
    child_context_op = child_context_ops[0]

    # Verify child operations are accessible
    child_operations = child_context_op.child_operations
    assert child_operations is not None
    assert len(child_operations) == 2  # wait + waitForCallback

    all_ops = result.get_all_operations()

    # Verify completed operations count
    completed_operations = [op for op in all_ops if op.status.value == "SUCCEEDED"]
    assert len(completed_operations) == 8

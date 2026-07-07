"""Tests for wait_for_callback_nested."""

import json

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.wait_for_callback import wait_for_callback_nested


async def test_handle_nested_wait_for_callback_operations_in_child_contexts(
    durable_runner,
):
    """Test nested waitForCallback operations in child contexts."""
    async with durable_runner(
        handler=wait_for_callback_nested.handler, input=None, timeout=60
    ) as runner:
        # Start the execution (this will pause at callbacks)
        execution_arn = await runner.run_async()

        # Complete outer callback first
        outer_callback_id = await runner.wait_for_callback(execution_arn=execution_arn)
        outer_callback_result = json.dumps({"level": "outer-completed"})
        await runner.send_callback_success(
            callback_id=outer_callback_id, result=outer_callback_result.encode()
        )

        # Complete inner callback
        inner_callback_id = await runner.wait_for_callback(
            execution_arn=execution_arn, name="inner-callback-op-callback"
        )
        inner_callback_result = json.dumps({"level": "inner-completed"})
        await runner.send_callback_success(
            callback_id=inner_callback_id, result=inner_callback_result.encode()
        )

        # Complete nested callback
        nested_callback_id = await runner.wait_for_callback(
            execution_arn=execution_arn, name="nested-callback-op-callback"
        )
        nested_callback_result = json.dumps({"level": "nested-completed"})
        await runner.send_callback_success(
            callback_id=nested_callback_id, result=nested_callback_result.encode()
        )

        # Wait for the execution to complete
        result = await runner.wait_for_result(execution_arn=execution_arn)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    assert result_data == {
        "outerCallback": outer_callback_result,
        "nestedResults": {
            "innerCallback": inner_callback_result,
            "deepNested": {
                "nestedCallback": nested_callback_result,
                "deepLevel": "inner-child",
            },
            "level": "outer-child",
        },
    }

    # Get all operations including nested ones
    all_ops = result.get_all_operations()

    # Find the outer context operation
    outer_context_op = result.get_context("outer-child-context")

    # Verify outer child operations hierarchy
    outer_children = result.get_child_operations(outer_context_op)
    assert outer_children is not None
    assert len(outer_children) == 2  # inner callback + inner context

    # Find the inner context operation
    inner_context_ops = [
        op
        for op in all_ops
        if op.operation_type.value == "CONTEXT" and op.name == "inner-child-context"
    ]
    assert len(inner_context_ops) == 1
    inner_context_op = inner_context_ops[0]

    # Verify inner child operations hierarchy
    inner_children = result.get_child_operations(inner_context_op)
    assert inner_children is not None
    assert len(inner_children) == 2  # deep wait + nested callback

    # Should have tracked all operations
    assert len(all_ops) == 12

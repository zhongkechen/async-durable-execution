"""Tests for block_example."""

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.block_example import block_example


def _get_all_operations(operations):
    """Recursively get all operations including nested ones."""
    all_ops = []
    for op in operations:
        all_ops.append(op)
        if hasattr(op, "child_operations") and op.child_operations:
            all_ops.extend(_get_all_operations(op.child_operations))
    return all_ops


async def test_block_example(durable_runner):
    """Test block example with nested child contexts."""
    with durable_runner(
        handler=block_example.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    # Verify the final result structure
    assert result.get_deserialized_result() == {
        "nestedStep": "nested step result",
        "nestedBlock": "nested block result",
    }

    # Check for the parent block operation
    parent_block_ops = [
        op
        for op in result.operations
        if op.operation_type.value == "CONTEXT" and op.name == "parent_block"
    ]
    assert len(parent_block_ops) == 1
    parent_block_op = parent_block_ops[0]

    # Verify parent block result
    assert parent_block_op.get_deserialized_result() == {
        "nestedStep": "nested step result",
        "nestedBlock": "nested block result",
    }

    # Verify parent block has 2 child operations
    child_operations = parent_block_op.child_operations
    assert len(child_operations) == 2

    # First child should be a STEP with result "nested step result"
    assert child_operations[0].operation_type.value == "STEP"
    assert child_operations[0].get_deserialized_result() == "nested step result"

    # Second child should be a CONTEXT with result "nested block result"
    assert child_operations[1].operation_type.value == "CONTEXT"
    assert child_operations[1].get_deserialized_result() == "nested block result"

    # Check for nested step operation by name
    nested_step_ops = [
        op
        for op in result.operations
        if op.operation_type.value == "STEP" and op.name == "nested_step"
    ]
    # Note: nested_step is inside parent_block, so it won't be at top level
    # We need to search in child operations
    all_ops = _get_all_operations(result.operations)
    nested_step_ops = [
        op
        for op in all_ops
        if op.operation_type.value == "STEP" and op.name == "nested_step"
    ]
    assert len(nested_step_ops) == 1
    assert nested_step_ops[0].get_deserialized_result() == "nested step result"

    # Check for nested block operation by name
    nested_block_ops = [
        op
        for op in all_ops
        if op.operation_type.value == "CONTEXT" and op.name == "nested_block"
    ]
    assert len(nested_block_ops) == 1
    assert nested_block_ops[0].get_deserialized_result() == "nested block result"

    # Verify wait operation exists within nested context
    wait_ops = [op for op in all_ops if op.operation_type.value == "WAIT"]
    assert len(wait_ops) >= 1

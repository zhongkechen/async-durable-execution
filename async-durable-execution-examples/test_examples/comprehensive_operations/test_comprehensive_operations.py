"""Tests for comprehensive_operations."""

from async_durable_execution import InvocationStatus
from async_durable_execution_examples.comprehensive_operations import (
    comprehensive_operations,
)


async def test_execute_all_operations_successfully(durable_runner):
    """Test that all operations execute successfully."""
    with durable_runner(
        handler=comprehensive_operations.handler, input={"message": "test"}, timeout=30
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()

    assert result_data["step1"] == "Step 1 completed successfully"
    assert result_data["waitCompleted"] is True

    # verify map results
    map_results = result_data["mapResults"]
    assert len(map_results["all"]) == 5
    assert [item["result"] for item in map_results["all"]] == [1, 2, 3, 4, 5]
    assert map_results["completionReason"] == "ALL_COMPLETED"

    # verify parallel results
    parallel_results = result_data["parallelResults"]
    assert len(parallel_results["all"]) == 3
    assert [item["result"] for item in parallel_results["all"]] == [
        "apple",
        "banana",
        "orange",
    ]
    assert parallel_results["completionReason"] == "ALL_COMPLETED"

    # Get all operations including nested ones
    all_ops = result.get_all_operations()

    # Verify step1 operation
    step1_ops = [
        op for op in all_ops if op.operation_type.value == "STEP" and op.name == "step1"
    ]
    assert len(step1_ops) == 1
    step1_op = step1_ops[0]
    assert result.get_operation_deserialized_result(step1_op) == "Step 1 completed successfully"

    # Verify wait operation (should be at index 1)
    wait_op = result.operations[1]
    assert wait_op.operation_type.value == "WAIT"

    # Verify individual map step operations exist with correct names
    for i in range(5):
        map_step_ops = [
            op
            for op in all_ops
            if op.operation_type.value == "STEP" and op.name == f"map-step-{i}"
        ]
        assert len(map_step_ops) == 1
        assert result.get_operation_deserialized_result(map_step_ops[0]) == i + 1

    # Verify individual parallel step operations exist
    fruit_step_1_ops = [
        op
        for op in all_ops
        if op.operation_type.value == "STEP" and op.name == "fruit-step-1"
    ]
    assert len(fruit_step_1_ops) == 1
    assert result.get_operation_deserialized_result(fruit_step_1_ops[0]) == "apple"

    fruit_step_2_ops = [
        op
        for op in all_ops
        if op.operation_type.value == "STEP" and op.name == "fruit-step-2"
    ]
    assert len(fruit_step_2_ops) == 1
    assert result.get_operation_deserialized_result(fruit_step_2_ops[0]) == "banana"

    fruit_step_3_ops = [
        op
        for op in all_ops
        if op.operation_type.value == "STEP" and op.name == "fruit-step-3"
    ]
    assert len(fruit_step_3_ops) == 1
    assert result.get_operation_deserialized_result(fruit_step_3_ops[0]) == "orange"

"""Tests for logger_example."""

from async_durable_execution.execution import InvocationStatus
from async_durable_execution.models import OperationType
from async_durable_execution_examples.logger_example import logger_example


async def test_logger_example(durable_runner):
    """Test logger example."""
    with durable_runner(
        handler=logger_example.handler, input={"id": "test-123"}, timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "processed-child-processed"

    # Verify step operations exist (process_data at top level)
    # Note: child_step is nested inside the CONTEXT operation, not at top level
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) >= 1

    # Verify context operation exists (child_workflow)
    context_ops = [
        op for op in result.operations if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1

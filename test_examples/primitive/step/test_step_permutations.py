"""Tests for step operation permutations."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationType
from examples.primitive.step import (
    step_no_name,
    step_with_exponential_backoff,
    step_with_name,
)


async def test_step_no_name(durable_runner):
    """Test step without explicit name."""
    async with durable_runner(
        handler=step_no_name.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "Result: Step without name"

    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1
    assert step_ops[0].name == "unnamed_step"


async def test_step_with_name(durable_runner):
    """Test step with explicit name."""
    async with durable_runner(
        handler=step_with_name.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "Result: Step with explicit name"

    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1
    assert step_ops[0].name == "custom_step"


async def test_step_with_exponential_backoff(durable_runner):
    """Test step with exponential backoff retry strategy."""
    async with durable_runner(
        handler=step_with_exponential_backoff.handler, input="test", timeout=10
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == "Result: Step with exponential backoff"

    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1
    assert step_ops[0].name == "retry_step"

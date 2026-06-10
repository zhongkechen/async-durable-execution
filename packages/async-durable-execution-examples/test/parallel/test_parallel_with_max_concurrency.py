"""Tests for parallel with maxConcurrency."""

import pytest
from async_durable_execution.execution import InvocationStatus
from async_durable_execution.lambda_service import OperationStatus
from async_durable_execution_examples.parallel import parallel_with_max_concurrency
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=parallel_with_max_concurrency.handler,
)
def test_parallel_with_max_concurrency(durable_runner):
    """Test parallel with maxConcurrency limit."""
    with durable_runner:
        result = durable_runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    results_list = deserialize_operation_payload(result.result)
    assert len(results_list) == 5
    assert set(results_list) == {"task 1", "task 2", "task 3", "task 4", "task 5"}

    # Get the parallel operation
    parallel_op = result.get_context("parallel_with_concurrency")
    assert parallel_op is not None
    assert parallel_op.status is OperationStatus.SUCCEEDED

    # Verify all 5 child operations exist
    assert len(parallel_op.child_operations) == 5

    # Verify all children succeeded
    for child in parallel_op.child_operations:
        assert child.status is OperationStatus.SUCCEEDED

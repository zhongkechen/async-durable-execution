"""Additional concurrent tests for wait and retry operations."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from async_durable_execution.models import (
    Operation,
    OperationStatus,
    OperationType,
    StepDetails,
)
from async_durable_execution.runner.execution import Execution
from async_durable_execution.runner.model import StartDurableExecutionInput


def test_concurrent_wait_and_retry_completion():
    """Test concurrent complete_wait and complete_retry operations."""
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-inv-id",
        input='{"test": "data"}',
    )
    execution = Execution.new(input_data)

    # Add WAIT and STEP operations
    wait_op = Operation(
        operation_id="wait-1",
        parent_id=None,
        name="test-wait",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.WAIT,
        status=OperationStatus.STARTED,
    )

    step_op = Operation(
        operation_id="step-1",
        parent_id=None,
        name="test-step",
        start_timestamp=datetime.now(timezone.utc),
        operation_type=OperationType.STEP,
        status=OperationStatus.PENDING,
        step_details=StepDetails(),
    )

    execution.operations.extend([wait_op, step_op])

    def complete_wait():
        result = execution.complete_wait("wait-1")
        return f"wait-completed-{result.status.value}"

    def complete_retry():
        result = execution.complete_retry("step-1")
        return f"retry-completed-{result.status.value}"

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        futures.append(executor.submit(complete_wait))
        futures.append(executor.submit(complete_retry))

        results = [future.result() for future in as_completed(futures)]

    assert len(results) == 2
    assert "wait-completed-SUCCEEDED" in results
    assert "retry-completed-READY" in results

    # Verify token sequence was incremented twice
    assert execution.token_sequence == 2

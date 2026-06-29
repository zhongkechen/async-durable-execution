"""Concurrent access tests for Execution class."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from async_durable_execution.runner.execution import Execution
from async_durable_execution.runner.model import StartDurableExecutionInput


def test_concurrent_token_generation():
    """Test concurrent checkpoint token generation."""
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

    def generate_token():
        return execution.get_new_checkpoint_token()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(generate_token) for _ in range(20)]
        tokens = [future.result() for future in as_completed(futures)]

    # All tokens should be unique and sequential
    assert len(tokens) == 20
    assert len(set(tokens)) == 20  # All unique
    assert execution.token_sequence == 20


def test_concurrent_operations_modification():
    """Test concurrent operations list modifications."""
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

    def start_execution():
        execution.start()
        return "started"

    def get_operations():
        ops = execution.get_navigable_operations()
        return f"ops-{len(ops)}"

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        # One start operation
        futures.append(executor.submit(start_execution))
        # Multiple read operations
        futures.extend([executor.submit(get_operations) for _ in range(4)])

        results = [future.result() for future in as_completed(futures)]

    assert len(results) == 5
    assert "started" in results
    # Should have at least one operation after start
    final_ops = execution.get_navigable_operations()
    assert len(final_ops) >= 1

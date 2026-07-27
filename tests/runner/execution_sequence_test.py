"""Single-threaded state transition tests for Execution class."""

from async_durable_execution._runner.local.execution import Execution
from async_durable_execution._runner.local.model import StartDurableExecutionInput


def test_checkpoint_token_generation_sequence():
    """Test checkpoint tokens are generated sequentially."""
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

    tokens = [execution.get_new_checkpoint_token() for _ in range(20)]

    # All tokens should be unique and sequential
    assert len(tokens) == 20
    assert len(set(tokens)) == 20  # All unique
    assert execution.token_sequence == 20


def test_operations_modification_sequence():
    """Test operations can be updated and read in the local runner thread."""
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

    execution.start()
    results = [f"ops-{len(execution.get_navigable_operations())}" for _ in range(4)]

    assert len(results) == 4
    final_ops = execution.get_navigable_operations()
    assert len(final_ops) == 1

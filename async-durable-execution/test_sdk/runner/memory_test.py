"""Tests for InMemoryExecutionStore."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone
from unittest.mock import Mock

import pytest

from async_durable_execution.runner.execution import Execution
from async_durable_execution.runner.model import StartDurableExecutionInput
from async_durable_execution.runner.local import InMemoryExecutionStore


def test_in_memory_execution_store_save_and_load():
    """Test saving and loading an execution."""
    store = InMemoryExecutionStore()
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)

    store.save(execution)
    loaded_execution = store.load(execution.durable_execution_arn)

    assert loaded_execution is execution


def test_in_memory_execution_store_load_nonexistent():
    """Test loading a nonexistent execution raises KeyError."""
    store = InMemoryExecutionStore()

    with pytest.raises(KeyError):
        store.load("nonexistent-arn")


def test_in_memory_execution_store_update():
    """Test updating an execution."""
    store = InMemoryExecutionStore()
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution = Execution.new(input_data)
    store.save(execution)

    execution.is_complete = True
    store.update(execution)

    loaded_execution = store.load(execution.durable_execution_arn)
    assert loaded_execution.is_complete is True


def test_in_memory_execution_store_update_overwrites():
    """Test that update overwrites existing execution."""
    store = InMemoryExecutionStore()
    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    execution1 = Execution.new(input_data)
    execution2 = Execution.new(input_data)
    execution2.durable_execution_arn = execution1.durable_execution_arn

    store.save(execution1)
    store.update(execution2)

    loaded_execution = store.load(execution1.durable_execution_arn)
    assert loaded_execution is execution2


def test_in_memory_execution_store_multiple_executions():
    """Test storing multiple executions."""
    store = InMemoryExecutionStore()
    input_data1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function-1",
        function_qualifier="$LATEST",
        execution_name="test-execution-1",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
    )
    input_data2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function-2",
        function_qualifier="$LATEST",
        execution_name="test-execution-2",
        execution_timeout_seconds=600,
        execution_retention_period_days=14,
    )

    execution1 = Execution.new(input_data1)
    execution2 = Execution.new(input_data2)

    store.save(execution1)
    store.save(execution2)

    loaded_execution1 = store.load(execution1.durable_execution_arn)
    loaded_execution2 = store.load(execution2.durable_execution_arn)

    assert loaded_execution1 is execution1
    assert loaded_execution2 is execution2


def test_in_memory_execution_store_list_all_empty():
    """Test list_all method with empty store."""
    store = InMemoryExecutionStore()

    result = store.list_all()

    assert result == []


def test_in_memory_execution_store_list_all_with_executions():
    """Test list_all method with multiple executions."""
    store = InMemoryExecutionStore()

    # Create test executions
    execution1 = Mock()
    execution1.durable_execution_arn = "arn1"
    execution2 = Mock()
    execution2.durable_execution_arn = "arn2"
    execution3 = Mock()
    execution3.durable_execution_arn = "arn3"

    # Save executions
    store.save(execution1)
    store.save(execution2)
    store.save(execution3)

    # Test list_all
    result = store.list_all()

    assert len(result) == 3
    assert execution1 in result
    assert execution2 in result
    assert execution3 in result


def test_in_memory_execution_store_query_empty():
    """Test query method with empty store."""
    store = InMemoryExecutionStore()

    executions, next_marker = store.query()

    assert executions == []
    assert next_marker is None


def test_in_memory_execution_store_query_by_function_name():
    """Test query filtering by function name."""
    store = InMemoryExecutionStore()

    # Create executions with different function names
    input1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="function-a",
        function_qualifier="$LATEST",
        execution_name="exec-1",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-1",
    )
    input2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="function-b",
        function_qualifier="$LATEST",
        execution_name="exec-2",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-2",
    )

    exec1 = Execution.new(input1)
    exec1.start()
    exec2 = Execution.new(input2)
    exec2.start()
    store.save(exec1)
    store.save(exec2)

    # Query for function-a only
    executions, next_marker = store.query(function_name="function-a")

    assert len(executions) == 1
    assert executions[0] is exec1
    assert next_marker is None


def test_in_memory_execution_store_query_by_execution_name():
    """Test query filtering by execution name."""
    store = InMemoryExecutionStore()

    input1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="exec-alpha",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-1",
    )
    input2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="exec-beta",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-2",
    )

    exec1 = Execution.new(input1)
    exec1.start()
    exec2 = Execution.new(input2)
    exec2.start()
    store.save(exec1)
    store.save(exec2)

    executions, next_marker = store.query(execution_name="exec-beta")

    assert len(executions) == 1
    assert executions[0] is exec2


def test_in_memory_execution_store_query_by_status():
    """Test query filtering by status."""
    store = InMemoryExecutionStore()

    # Create running execution
    input1 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="running-exec",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-1",
    )
    exec1 = Execution.new(input1)
    exec1.start()

    # Create completed execution
    input2 = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="completed-exec",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="invocation-2",
    )
    exec2 = Execution.new(input2)
    exec2.start()
    exec2.complete_success("success result")

    store.save(exec1)
    store.save(exec2)

    # Query for running executions
    executions, next_marker = store.query(status_filter="RUNNING")

    assert len(executions) == 1
    assert executions[0] is exec1

    # Query for succeeded executions
    executions, next_marker = store.query(status_filter="SUCCEEDED")

    assert len(executions) == 1
    assert executions[0] is exec2


def test_in_memory_execution_store_query_pagination():
    """Test query pagination."""
    store = InMemoryExecutionStore()

    # Create multiple executions
    executions = []
    for i in range(5):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"exec-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"invocation-{i}",
        )
        exec_obj = Execution.new(input_data)
        exec_obj.start()
        executions.append(exec_obj)
        store.save(exec_obj)

    # Test first page
    executions, next_marker = store.query(limit=2, offset=0)

    assert len(executions) == 2
    assert next_marker is not None

    # Test second page
    executions, next_marker = store.query(limit=2, offset=2)

    assert len(executions) == 2
    assert next_marker is not None

    # Test last page
    executions, next_marker = store.query(limit=2, offset=4)

    assert len(executions) == 1
    assert next_marker is None


def test_in_memory_execution_store_query_sorting():
    """Test query sorting by timestamp."""
    store = InMemoryExecutionStore()

    # Create executions - they will be sorted by creation order
    executions = []
    for i in range(3):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"exec-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"invocation-{i}",
        )
        exec_obj = Execution.new(input_data)
        exec_obj.start()
        executions.append(exec_obj)
        store.save(exec_obj)

    # Test ascending order (default)
    executions, next_marker = store.query(reverse_order=False)

    assert len(executions) == 3

    # Test descending order
    executions, next_marker = store.query(reverse_order=True)

    assert len(executions) == 3


def test_in_memory_execution_store_query_combined_filters():
    """Test query with multiple filters combined."""
    store = InMemoryExecutionStore()

    # Create various executions
    inputs = [
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="function-a",
            function_qualifier="$LATEST",
            execution_name="target-exec",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="invocation-1",
        ),
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="function-b",
            function_qualifier="$LATEST",
            execution_name="target-exec",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="invocation-2",
        ),
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="function-a",
            function_qualifier="$LATEST",
            execution_name="other-exec",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id="invocation-3",
        ),
    ]

    executions = []
    for input_data in inputs:
        exec_obj = Execution.new(input_data)
        exec_obj.start()
        executions.append(exec_obj)
        store.save(exec_obj)

    # Query with both function_name and execution_name filters
    filtered_executions, next_marker = store.query(
        function_name="function-a", execution_name="target-exec"
    )

    assert len(filtered_executions) == 1
    assert filtered_executions[0] is executions[0]


def test_time_filtering_logic():
    """Test time filtering logic in process_query method."""
    from datetime import datetime
    from unittest.mock import Mock

    store = InMemoryExecutionStore()

    # Create mock executions with different timestamps
    exec1 = Mock()
    exec1.start_input.function_name = "test-function"
    exec1.start_input.execution_name = "exec1"
    exec1.status = "RUNNING"

    exec2 = Mock()
    exec2.start_input.function_name = "test-function"
    exec2.start_input.execution_name = "exec2"
    exec2.status = "RUNNING"

    exec3 = Mock()
    exec3.start_input.function_name = "test-function"
    exec3.start_input.execution_name = "exec3"
    exec3.status = "RUNNING"

    # Use real datetime objects for timestamps
    op1 = Mock()
    op1.start_timestamp = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    op2 = Mock()
    op2.start_timestamp = datetime(2023, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    op3 = Mock()
    op3.start_timestamp = datetime(2023, 1, 3, 12, 0, 0)  # noqa: DTZ001

    exec1.get_operation_execution_started.return_value = op1
    exec2.get_operation_execution_started.return_value = op2
    exec3.get_operation_execution_started.return_value = op3

    executions = [exec1, exec2, exec3]

    # Test time_after filtering
    filtered, _ = store.process_query(
        executions,
        started_after="1672617600.0",  # 2023-01-01 24:00:00 UTC (between exec1 and exec2)
    )
    assert len(filtered) == 2
    assert exec2 in filtered
    assert exec3 in filtered
    assert exec1 not in filtered

    # Test time_before filtering
    filtered, _ = store.process_query(
        executions,
        started_before="1672617600.0",  # 2023-01-01 24:00:00 UTC
    )
    assert len(filtered) == 1
    assert exec1 in filtered
    assert exec2 not in filtered
    assert exec3 not in filtered

    # Test both time_after and time_before
    filtered, _ = store.process_query(
        executions,
        started_after="1672617600.0",  # 2023-01-02 00:00:00 UTC (between exec1 and exec2)
        started_before="1672704000.0",  # 2023-01-03 00:00:00 UTC (between exec2 and exec3)
    )
    assert len(filtered) == 1
    assert exec2 in filtered

    # Test exception handling - exec with AttributeError
    exec_error = Mock()
    exec_error.start_input.function_name = "test-function"
    exec_error.start_input.execution_name = "exec_error"
    exec_error.status = "RUNNING"
    exec_error.get_operation_execution_started.side_effect = AttributeError(
        "No operation"
    )

    executions_with_error = [exec1, exec_error, exec2]
    filtered, _ = store.process_query(
        executions_with_error,
        started_after="1672617600.0",  # After exec1, before exec2
    )
    # exec_error should be filtered out due to exception, only exec2 should remain
    assert len(filtered) == 1
    assert exec2 in filtered
    assert exec_error not in filtered


# Concurrent memory tests
def test_concurrent_save_load():
    """Test concurrent save and load operations."""
    store = InMemoryExecutionStore()

    def save_execution(i: int):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"test-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
            input=f'{{"test": {i}}}',
        )
        execution = Execution.new(input_data)
        execution.durable_execution_arn = f"arn-{i}"
        store.save(execution)
        return f"saved-{i}"

    def load_execution(i: int):
        try:
            execution = store.load(f"arn-{i}")
            return f"loaded-{execution.start_input.execution_name}"
        except KeyError:
            return f"not-found-{i}"

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit save operations first
        futures = [executor.submit(save_execution, i) for i in range(5)]
        # Wait for saves to complete
        save_results = [future.result() for future in as_completed(futures)]

        # Then submit load operations
        futures = [executor.submit(load_execution, i) for i in range(5)]
        # Wait for loads to complete
        load_results = [future.result() for future in as_completed(futures)]

    results = save_results + load_results
    assert len(results) == 10


def test_concurrent_update_list():
    """Test concurrent update and list operations."""
    store = InMemoryExecutionStore()

    # Pre-populate store
    for i in range(3):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name="test-function",
            function_qualifier="$LATEST",
            execution_name=f"test-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
            input=f'{{"test": {i}}}',
        )
        execution = Execution.new(input_data)
        execution.durable_execution_arn = f"arn-{i}"
        store.save(execution)

    def update_execution(i: int):
        execution = store.load(f"arn-{i}")
        execution.is_complete = True
        store.update(execution)
        return f"updated-{i}"

    def list_stored_executions():
        executions = store.list_all()
        return f"listed-{len(executions)}"

    with ThreadPoolExecutor(max_workers=6) as executor:
        # Submit update operations
        futures = [executor.submit(update_execution, i) for i in range(3)]
        # Submit list operations
        futures.extend([executor.submit(list_stored_executions) for _ in range(3)])

        # Wait for all operations to complete
        results = [future.result() for future in as_completed(futures)]

    assert len(results) == 6
    final_list = store.list_all()
    assert len(final_list) == 3


def test_concurrent_query_operations():
    """Test concurrent query operations on memory store."""
    store = InMemoryExecutionStore()

    # Pre-populate store with test data
    for i in range(10):
        input_data = StartDurableExecutionInput(
            account_id="123456789012",
            function_name=f"function-{i % 3}",  # 3 different functions
            function_qualifier="$LATEST",
            execution_name=f"exec-{i}",
            execution_timeout_seconds=300,
            execution_retention_period_days=7,
            invocation_id=f"inv-{i}",
        )
        execution = Execution.new(input_data)
        execution.start()
        # Complete some executions
        if i % 4 == 0:
            execution.complete_success("success")
        store.save(execution)

    def query_store(query_type: str):
        if query_type == "function":
            executions, next_marker = store.query(function_name="function-1")
        elif query_type == "status":
            executions, next_marker = store.query(status_filter="SUCCEEDED")
        elif query_type == "pagination":
            executions, next_marker = store.query(limit=3, offset=2)
        else:
            executions, next_marker = store.query()

        return f"{query_type}-{len(executions)}"

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(query_store, "function"),
            executor.submit(query_store, "status"),
            executor.submit(query_store, "pagination"),
            executor.submit(query_store, "all"),
        ]
        results = [future.result() for future in as_completed(futures)]

    assert len(results) == 4

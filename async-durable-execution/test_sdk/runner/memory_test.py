"""Tests for InMemoryExecutionStore."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import Mock

import pytest

from async_durable_execution.runner.execution import Execution
from async_durable_execution.runner.model import StartDurableExecutionInput
from async_durable_execution.runner.memory import (
    InMemoryExecutionStore,
)


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

"""Concurrent access tests for execution stores."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from async_durable_execution.runner.execution import Execution
from async_durable_execution.runner.model import StartDurableExecutionInput
from async_durable_execution.runner.stores.memory import (
    InMemoryExecutionStore,
)


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

    def list_executions():
        executions = store.list_all()
        return f"listed-{len(executions)}"

    with ThreadPoolExecutor(max_workers=6) as executor:
        # Submit update operations
        futures = [executor.submit(update_execution, i) for i in range(3)]
        # Submit list operations
        futures.extend([executor.submit(list_executions) for _ in range(3)])

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

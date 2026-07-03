from __future__ import annotations

import asyncio
import inspect
import time
from datetime import timezone
from threading import Lock
from typing import Any, Callable

from ...client import DurableServiceClient
from ...execution import (
    DurableExecutionInvocationInput,
    InitialExecutionState,
    _bind_service_client_to_handler,
)
from ...models import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    DurableExecutionInvocationOutput,
    ErrorObject,
    Operation,
    OperationUpdate,
    StateOutput,
)
from ..exceptions import (
    DurableFunctionsTestError,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from .execution import Execution
from .executor import Executor
from ..model import (
    DurableFunctionTestResult,
    InvokeResponse,
    _get_callback_id_from_events,
)
from .model import (
    CheckpointToken,
    Invoker,
    LambdaContext,
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
)
from .processor import (
    CheckpointValidator,
    OperationTransformer,
)
from .scheduler import Event, Scheduler

__all__ = [
    "DurableFunctionLocalTestRunner",
    "Event",
    "Executor",
    "InMemoryExecutionStore",
    "InMemoryServiceClient",
    "InProcessInvoker",
    "Scheduler",
    "create_local_runner",
    "create_test_lambda_context",
]


def create_local_runner(
    *,
    handler: Callable,
    poll_interval: float = 1.0,
    input: Any = None,  # noqa: A002
    timeout: int = 60,
) -> DurableFunctionLocalTestRunner:
    """Create a configured local durable function runner."""
    return DurableFunctionLocalTestRunner(
        handler=handler,
        poll_interval=poll_interval,
        input=input,
        timeout=timeout,
    )


class DurableFunctionLocalTestRunner:
    def __init__(
        self,
        handler: Callable,
        poll_interval: float = 1.0,
        input: Any = None,  # noqa: A002
        timeout: int = 900,
        function_name: str = "test-function",
        execution_name: str = "execution-name",
        account_id: str = "123456789012",
    ):
        self._scheduler: Scheduler = Scheduler()
        self._scheduler.start()
        self._store = InMemoryExecutionStore()
        self.mode = "local"
        self.poll_interval = poll_interval
        self._default_input = input
        self._default_timeout = timeout
        self._function_name = function_name
        self._execution_name = execution_name
        self._account_id = account_id
        self._service_client = InMemoryServiceClient(
            store=self._store, scheduler=self._scheduler
        )
        self._invoker = InProcessInvoker(handler, self._service_client)
        self._executor = Executor(
            store=self._store,
            scheduler=self._scheduler,
            invoker=self._invoker,
            service_client=self._service_client,
        )

        self._service_client.bind_executor(self._executor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def __aenter__(self) -> DurableFunctionLocalTestRunner:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self):
        self._scheduler.stop()

    async def run(
        self,
    ) -> DurableFunctionTestResult:
        execution_arn = await self.run_async()
        return await self.wait_for_result(
            execution_arn=execution_arn, timeout=self._default_timeout
        )

    async def send_callback_success(
        self, callback_id: str, result: bytes | None = None
    ) -> None:
        self._executor.send_callback_success(callback_id=callback_id, result=result)

    async def send_callback_failure(
        self, callback_id: str, error: ErrorObject | None = None
    ) -> None:
        self._executor.send_callback_failure(callback_id=callback_id, error=error)

    async def send_callback_heartbeat(self, callback_id: str) -> None:
        self._executor.send_callback_heartbeat(callback_id=callback_id)

    def mock_invoke_result(self, function_name: str, result: Any) -> None:
        """Register a local mock result for a chained invoke function."""
        self._service_client.mock_invoke_result(
            function_name=function_name,
            result=result,
        )

    async def run_async(
        self,
    ) -> str:
        start_input = StartDurableExecutionInput(
            account_id=self._account_id,
            function_name=self._function_name,
            function_qualifier="$LATEST",
            execution_name=self._execution_name,
            execution_timeout_seconds=self._default_timeout,
            execution_retention_period_days=7,
            invocation_id="inv-12345678-1234-1234-1234-123456789012",
            trace_fields={"trace_id": "abc123", "span_id": "def456"},
            tenant_id="tenant-001",
            input=self._default_input,
        )

        output: StartDurableExecutionOutput = self._executor.start_execution(
            start_input
        )

        if output.execution_arn is None:
            msg_arn: str = "Execution ARN must exist to run test."
            raise DurableFunctionsTestError(msg_arn)
        return output.execution_arn

    async def wait_for_result(
        self, execution_arn: str, timeout: int = 60
    ) -> DurableFunctionTestResult:
        completed = self._executor.wait_until_complete(execution_arn, timeout)

        if not completed:
            msg_timeout: str = "Execution did not complete within timeout"

            raise TimeoutError(msg_timeout)

        execution: Execution = self._store.load(execution_arn)
        return DurableFunctionTestResult.create(execution=execution)

    async def wait_for_callback(
        self, execution_arn: str, name: str | None = None, timeout: int = 60
    ) -> str:
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                history_response = self._executor.get_execution_history(execution_arn)
                callback_id = _get_callback_id_from_events(
                    events=history_response.events, name=name
                )
                if callback_id:
                    return callback_id
            except ResourceNotFoundException:
                pass
            except Exception as e:
                msg = f"Failed to fetch execution history: {e}"
                raise DurableFunctionsTestError(msg) from e

            await asyncio.sleep(self.poll_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        msg = f"Callback was not available within {timeout}s (elapsed: {elapsed:.1f}s)."
        raise TimeoutError(msg)


class InProcessInvoker(Invoker):
    def __init__(self, handler: Callable, service_client: InMemoryServiceClient):
        self.handler = _bind_service_client_to_handler(handler, service_client)
        self.service_client = service_client

    def create_invocation_input(
        self,
        *,
        start_input: StartDurableExecutionInput,  # noqa: ARG002
        durable_execution_arn: str,
        checkpoint_token: str,
        operations: list[Operation],
    ) -> DurableExecutionInvocationInput:
        return DurableExecutionInvocationInput(
            durable_execution_arn=durable_execution_arn,
            checkpoint_token=checkpoint_token,
            initial_execution_state=InitialExecutionState(
                operations=operations,
                next_marker="",
            ),
        )

    async def invoke(
        self,
        function_name: str,  # noqa: ARG002
        input: DurableExecutionInvocationInput,
        endpoint_url: str | None = None,  # noqa: ARG002
    ) -> InvokeResponse:
        context = create_test_lambda_context()
        payload = input.to_json_dict()
        async_handler = getattr(self.handler, "_async_handler", None)
        handler_result = (
            async_handler(payload, context)
            if inspect.iscoroutinefunction(async_handler)
            else self.handler(payload, context)
        )
        if inspect.isawaitable(handler_result):
            handler_result = await handler_result
        output = DurableExecutionInvocationOutput.from_dict(handler_result)
        return InvokeResponse(
            invocation_output=output, request_id=context.aws_request_id
        )

    def update_endpoint(self, endpoint_url: str, region_name: str) -> None:
        """No-op for in-process invoker."""


def create_test_lambda_context() -> LambdaContext:
    # Create client context as a dictionary, not as objects
    # LambdaContext.__init__ expects dictionaries and will create the objects internally
    client_context_dict = {
        "custom": {"test_key": "test_value"},
        "env": {"platform": "test", "make": "test", "model": "test"},
        "client": {
            "installation_id": "test-installation-123",
            "app_title": "TestApp",
            "app_version_name": "1.0.0",
            "app_version_code": "100",
            "app_package_name": "com.test.app",
        },
    }

    cognito_identity_dict = {
        "cognitoIdentityId": "test-cognito-identity-123",
        "cognitoIdentityPoolId": "us-west-2:test-pool-456",
    }

    return LambdaContext(
        aws_request_id="test-invoke-12345",
        client_context=client_context_dict,
        identity=cognito_identity_dict,
        invoked_function_arn="arn:aws:lambda:us-west-2:123456789012:function:test-function",
        tenant_id="test-tenant-789",
    )


class InMemoryExecutionStore:
    """Dict-based storage for testing."""

    def __init__(self) -> None:
        self._store: dict[str, Execution] = {}
        self._lock: Lock = Lock()

    def save(self, execution: Execution) -> None:
        with self._lock:
            self._store[execution.durable_execution_arn] = execution

    def load(self, execution_arn: str) -> Execution:
        with self._lock:
            return self._store[execution_arn]

    def update(self, execution: Execution) -> None:
        with self._lock:
            self._store[execution.durable_execution_arn] = execution

    def list_all(self) -> list[Execution]:
        with self._lock:
            return list(self._store.values())

    def query(
        self,
        function_name: str | None = None,
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> tuple[list[Execution], str | None]:
        """Apply filtering, sorting, and pagination to executions."""
        executions: list[Execution] = self.list_all()
        return self.process_query(
            executions,
            function_name=function_name,
            execution_name=execution_name,
            status_filter=status_filter,
            started_after=started_after,
            started_before=started_before,
            limit=limit,
            offset=offset,
            reverse_order=reverse_order,
        )

    @staticmethod
    def process_query(
        executions: list[Execution],
        function_name: str | None = None,
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> tuple[list[Execution], str | None]:
        """Apply filtering, sorting, and pagination to executions."""
        filtered: list[Execution] = []
        for execution in executions:
            if function_name and execution.start_input.function_name != function_name:
                continue
            if (
                execution_name
                and execution.start_input.execution_name != execution_name
            ):
                continue

            if status_filter and execution.current_status().value != status_filter:
                continue

            if started_after or started_before:
                try:
                    operation: Operation = execution.get_operation_execution_started()
                    if operation.start_timestamp:
                        timestamp: float = (
                            operation.start_timestamp.timestamp()
                            if hasattr(operation.start_timestamp, "timestamp")
                            else operation.start_timestamp.replace(
                                tzinfo=timezone.utc
                            ).timestamp()
                        )
                        if started_after and timestamp < float(started_after):
                            continue
                        if started_before and timestamp > float(started_before):
                            continue
                except (ValueError, AttributeError):
                    continue

            filtered.append(execution)

        def get_sort_key(exe: Execution):
            try:
                op: Operation = exe.get_operation_execution_started()
                if op.start_timestamp:
                    return (
                        op.start_timestamp.timestamp()
                        if hasattr(op.start_timestamp, "timestamp")
                        else op.start_timestamp.replace(tzinfo=timezone.utc).timestamp()
                    )
            except Exception:  # noqa: BLE001, S110
                pass
            return 0

        filtered.sort(key=get_sort_key, reverse=reverse_order)

        if limit is not None and limit > 0:
            end_idx: int = offset + limit
            paginated: list[Execution] = filtered[offset:end_idx]
            has_more: bool = end_idx < len(filtered)
            next_marker: str | None = str(end_idx) if has_more else None
            return paginated, next_marker
        return filtered[offset:], None


class InMemoryServiceClient(DurableServiceClient):
    """An in-memory service client, that can replace the boto lambda service client."""

    def __init__(self, store: InMemoryExecutionStore, scheduler: Scheduler):
        self._store = store
        self._scheduler = scheduler
        self._executor = None
        self._transformer = OperationTransformer()

    def bind_executor(self, executor) -> None:
        """Bind the local executor that handles checkpoint side effects."""
        self._executor = executor

    def mock_invoke_result(self, function_name: str, result: object) -> None:
        """Register a local mock result for a chained invoke function."""
        self._transformer.mock_invoke_result(function_name=function_name, result=result)

    def process_checkpoint(
        self,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,  # noqa: ARG002
    ) -> CheckpointOutput:
        """Process checkpoint updates and return result with updated execution state."""
        token: CheckpointToken = CheckpointToken.from_str(checkpoint_token)
        execution: Execution = self._store.load(token.execution_arn)

        if execution.is_complete or token.token_sequence != execution.token_sequence:
            msg: str = "Invalid checkpoint token"
            raise InvalidParameterValueException(msg)

        CheckpointValidator.validate_input(
            updates, execution, processors=self._transformer.processors
        )

        if self._executor is None:
            msg = "Local executor is not bound to the service client."
            raise InvalidParameterValueException(msg)

        updated_operations, all_updates = self._transformer.process_updates(
            updates=updates,
            current_operations=execution.operations,
            runner=self._executor,
            execution_arn=token.execution_arn,
        )

        new_checkpoint_token = execution.get_new_checkpoint_token()
        execution.operations = updated_operations
        execution.updates.extend(all_updates)
        self._store.update(execution)

        return CheckpointOutput(
            checkpoint_token=new_checkpoint_token,
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=execution.get_navigable_operations(), next_marker=None
            ),
        )

    async def checkpoint(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # durable_execution_arn is not used in in-memory testing
        return self.process_checkpoint(checkpoint_token, updates, client_token)

    async def get_execution_state(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
        # durable_execution_arn is not used in in-memory testing
        token: CheckpointToken = CheckpointToken.from_str(checkpoint_token)
        execution: Execution = self._store.load(token.execution_arn)

        # TODO: paging when size or max
        return StateOutput(
            operations=execution.get_navigable_operations(), next_marker=None
        )

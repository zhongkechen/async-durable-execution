from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    Self,
    TypeVar,
    cast,
)

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from async_durable_execution.execution import InvocationStatus
from async_durable_execution.lambda_service import (
    ErrorObject,
    OperationPayload,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from async_durable_execution.lambda_service import Operation as SvcOperation
from async_durable_execution.serdes import ExtendedTypeSerDes
from async_durable_execution_runner.checkpoint.processor import (
    CheckpointProcessor,
)
from async_durable_execution_runner.client import InMemoryServiceClient
from async_durable_execution_runner.exceptions import (
    DurableFunctionsLocalRunnerError,
    DurableFunctionsTestError,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from async_durable_execution_runner.executor import Executor
from async_durable_execution_runner.invoker import (
    InProcessInvoker,
    LambdaInvoker,
    create_lambda_client,
)
from async_durable_execution_runner.model import (
    GetDurableExecutionHistoryResponse,
    GetDurableExecutionResponse,
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
    events_to_operations,
)
from async_durable_execution_runner.scheduler import Scheduler
from async_durable_execution_runner.stores.base import (
    ExecutionStore,
    StoreType,
)
from async_durable_execution_runner.stores.filesystem import (
    FileSystemExecutionStore,
)
from async_durable_execution_runner.stores.memory import (
    InMemoryExecutionStore,
)
from async_durable_execution_runner.stores.sqlite import SQLiteExecutionStore
from async_durable_execution_runner.web.server import WebServer


if TYPE_CHECKING:
    import datetime
    from collections.abc import Callable, MutableMapping

    from async_durable_execution_runner.execution import Execution
    from async_durable_execution_runner.model import Event
    from async_durable_execution_runner.web.server import WebServiceConfig


logger = logging.getLogger(__name__)


def _deserialize_operation_payload(
    payload: OperationPayload | None,
    serdes: ExtendedTypeSerDes | None = None,
) -> Any:
    """Deserialize an operation payload using the provided or default serializer."""
    if not payload:
        return None

    if serdes is None:
        serdes = ExtendedTypeSerDes()

    try:
        return serdes.deserialize(payload)
    except Exception:
        return json.loads(payload)


@dataclass(frozen=True)
class WebRunnerConfig:
    """Configuration for the WebRunner using composition pattern.

    This configuration class encapsulates all settings needed to run the web server
    for durable functions testing, including HTTP server configuration and Lambda
    service configuration.
    """

    # HTTP server configuration (existing WebServiceConfig)
    web_service: WebServiceConfig

    # Lambda service configuration (web runner specific)
    lambda_endpoint: str = "http://127.0.0.1:3001"
    local_runner_endpoint: str = "http://0.0.0.0:5000"
    local_runner_region: str = "us-west-2"
    local_runner_mode: str = "local"

    # Store configuration
    store_type: StoreType = StoreType.MEMORY
    store_path: str | None = None  # Path for filesystem store


@dataclass(frozen=True)
class Operation:
    operation_id: str
    operation_type: OperationType
    status: OperationStatus
    parent_id: str | None = field(default=None, kw_only=True)
    name: str | None = field(default=None, kw_only=True)
    sub_type: OperationSubType | None = field(default=None, kw_only=True)
    start_timestamp: datetime.datetime | None = field(default=None, kw_only=True)
    end_timestamp: datetime.datetime | None = field(default=None, kw_only=True)


T = TypeVar("T", bound=Operation)


class OperationFactory(Protocol):
    @staticmethod
    def from_svc_operation(
        operation: SvcOperation, all_operations: list[SvcOperation] | None = None
    ) -> Operation: ...


@dataclass(frozen=True)
class ExecutionOperation(Operation):
    input_payload: str | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation,
        all_operations: list[SvcOperation] | None = None,  # noqa: ARG004
    ) -> ExecutionOperation:
        if operation.operation_type != OperationType.EXECUTION:
            msg: str = f"Expected EXECUTION operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)
        return ExecutionOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            input_payload=(
                operation.execution_details.input_payload
                if operation.execution_details
                else None
            ),
        )


@dataclass(frozen=True)
class ContextOperation(Operation):
    child_operations: list[Operation]
    result: OperationPayload | None = None
    error: ErrorObject | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation, all_operations: list[SvcOperation] | None = None
    ) -> ContextOperation:
        if operation.operation_type != OperationType.CONTEXT:
            msg: str = f"Expected CONTEXT operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)

        child_operations = []
        if all_operations:
            child_operations = [
                create_operation(op, all_operations)
                for op in all_operations
                if op.parent_id == operation.operation_id
            ]

        return ContextOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            child_operations=child_operations,
            result=operation.context_details.result
            if operation.context_details
            else None,
            error=operation.context_details.error
            if operation.context_details
            else None,
        )

    def get_operation_by_name(self, name: str) -> Operation:
        for operation in self.child_operations:
            if operation.name == name:
                return operation
        msg: str = f"Child Operation with name '{name}' not found"
        raise DurableFunctionsTestError(msg)

    def get_step(self, name: str) -> StepOperation:
        return cast("StepOperation", self.get_operation_by_name(name))

    def get_wait(self, name: str) -> WaitOperation:
        return cast("WaitOperation", self.get_operation_by_name(name))

    def get_context(self, name: str) -> ContextOperation:
        return cast("ContextOperation", self.get_operation_by_name(name))

    def get_callback(self, name: str) -> CallbackOperation:
        return cast("CallbackOperation", self.get_operation_by_name(name))

    def get_invoke(self, name: str) -> InvokeOperation:
        return cast("InvokeOperation", self.get_operation_by_name(name))

    def get_execution(self, name: str) -> ExecutionOperation:
        return cast("ExecutionOperation", self.get_operation_by_name(name))

    def get_deserialized_result(self, serdes: ExtendedTypeSerDes | None = None) -> Any:
        """Return the deserialized operation result."""
        return _deserialize_operation_payload(self.result, serdes)


@dataclass(frozen=True)
class StepOperation(ContextOperation):
    attempt: int = 0
    next_attempt_timestamp: datetime.datetime | None = None
    result: OperationPayload | None = None
    error: ErrorObject | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation, all_operations: list[SvcOperation] | None = None
    ) -> StepOperation:
        if operation.operation_type != OperationType.STEP:
            msg: str = f"Expected STEP operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)

        child_operations = []
        if all_operations:
            child_operations = [
                create_operation(op, all_operations)
                for op in all_operations
                if op.parent_id == operation.operation_id
            ]

        return StepOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            child_operations=child_operations,
            attempt=operation.step_details.attempt if operation.step_details else 0,
            next_attempt_timestamp=(
                operation.step_details.next_attempt_timestamp
                if operation.step_details
                else None
            ),
            result=operation.step_details.result if operation.step_details else None,
            error=operation.step_details.error if operation.step_details else None,
        )


@dataclass(frozen=True)
class WaitOperation(Operation):
    scheduled_end_timestamp: datetime.datetime | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation,
        all_operations: list[SvcOperation] | None = None,  # noqa: ARG004
    ) -> WaitOperation:
        if operation.operation_type != OperationType.WAIT:
            msg: str = f"Expected WAIT operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)
        return WaitOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            scheduled_end_timestamp=(
                operation.wait_details.scheduled_end_timestamp
                if operation.wait_details
                else None
            ),
        )


@dataclass(frozen=True)
class CallbackOperation(ContextOperation):
    callback_id: str | None = None
    result: OperationPayload | None = None
    error: ErrorObject | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation, all_operations: list[SvcOperation] | None = None
    ) -> CallbackOperation:
        if operation.operation_type != OperationType.CALLBACK:
            msg: str = f"Expected CALLBACK operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)

        child_operations = []
        if all_operations:
            child_operations = [
                create_operation(op, all_operations)
                for op in all_operations
                if op.parent_id == operation.operation_id
            ]

        return CallbackOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            child_operations=child_operations,
            callback_id=(
                operation.callback_details.callback_id
                if operation.callback_details
                else None
            ),
            result=operation.callback_details.result
            if operation.callback_details
            else None,
            error=operation.callback_details.error
            if operation.callback_details
            else None,
        )


@dataclass(frozen=True)
class InvokeOperation(Operation):
    result: OperationPayload | None = None
    error: ErrorObject | None = None

    @staticmethod
    def from_svc_operation(
        operation: SvcOperation,
        all_operations: list[SvcOperation] | None = None,  # noqa: ARG004
    ) -> InvokeOperation:
        if operation.operation_type != OperationType.CHAINED_INVOKE:
            msg: str = f"Expected INVOKE operation, got {operation.operation_type}"
            raise InvalidParameterValueException(msg)
        return InvokeOperation(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            status=operation.status,
            parent_id=operation.parent_id,
            name=operation.name,
            sub_type=operation.sub_type,
            start_timestamp=operation.start_timestamp,
            end_timestamp=operation.end_timestamp,
            result=operation.chained_invoke_details.result
            if operation.chained_invoke_details
            else None,
            error=operation.chained_invoke_details.error
            if operation.chained_invoke_details
            else None,
        )

    def get_deserialized_result(self, serdes: ExtendedTypeSerDes | None = None) -> Any:
        """Return the deserialized operation result."""
        return _deserialize_operation_payload(self.result, serdes)


OPERATION_FACTORIES: MutableMapping[OperationType, type[OperationFactory]] = {
    OperationType.EXECUTION: ExecutionOperation,
    OperationType.CONTEXT: ContextOperation,
    OperationType.STEP: StepOperation,
    OperationType.WAIT: WaitOperation,
    OperationType.CHAINED_INVOKE: InvokeOperation,
    OperationType.CALLBACK: CallbackOperation,
}


def create_operation(
    svc_operation: SvcOperation, all_operations: list[SvcOperation] | None = None
) -> Operation:
    operation_class: type[OperationFactory] | None = OPERATION_FACTORIES.get(
        svc_operation.operation_type
    )
    if not operation_class:
        msg: str = f"Unknown operation type: {svc_operation.operation_type}"
        raise DurableFunctionsTestError(msg)
    return operation_class.from_svc_operation(svc_operation, all_operations)


def _get_callback_id_from_events(
    events: list[Event], name: str | None = None
) -> str | None:
    """
    Get callback ID from execution history for callbacks that haven't completed.

    Args:
        execution_arn: The ARN of the execution to query.
        name: Optional callback name to search for. If not provided, returns the latest callback.

    Returns:
        The callback ID string for a non-completed callback, or None if not found.

    Raises:
        DurableFunctionsTestError: If the named callback has already succeeded/failed/timed out.
    """
    callback_started_events = [
        event for event in events if event.event_type == "CallbackStarted"
    ]

    if not callback_started_events:
        return None

    completed_callback_ids = {
        event.event_id
        for event in events
        if event.event_type
        in ["CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"]
    }

    if name is not None:
        for event in callback_started_events:
            if event.name == name:
                callback_id = event.event_id
                if callback_id in completed_callback_ids:
                    raise DurableFunctionsTestError(
                        f"Callback {name} has already completed (succeeded/failed/timed out)"
                    )
                return (
                    event.callback_started_details.callback_id
                    if event.callback_started_details
                    else None
                )
        return None

    # If name is not provided, find the latest non-completed callback event
    active_callbacks = [
        event
        for event in callback_started_events
        if event.event_id not in completed_callback_ids
    ]

    if not active_callbacks:
        return None

    latest_event = active_callbacks[-1]
    return (
        latest_event.callback_started_details.callback_id
        if latest_event.callback_started_details
        else None
    )


@dataclass(frozen=True)
class DurableFunctionTestResult:
    status: InvocationStatus
    operations: list[Operation]
    result: OperationPayload | None = None
    error: ErrorObject | None = None

    @classmethod
    def create(cls, execution: Execution) -> DurableFunctionTestResult:
        operations = []
        for operation in execution.operations:
            if operation.operation_type is OperationType.EXECUTION:
                # don't want the EXECUTION operations in the list test code asserts against
                continue

            if operation.parent_id is None:
                operations.append(create_operation(operation, execution.operations))

        if execution.result is None:
            msg: str = "Execution result must exist to create test result."
            raise DurableFunctionsTestError(msg)

        return cls(
            status=execution.result.status,
            operations=operations,
            result=execution.result.result,
            error=execution.result.error,
        )

    @classmethod
    def from_execution_history(
        cls,
        execution_response: GetDurableExecutionResponse,
        history_response: GetDurableExecutionHistoryResponse,
    ) -> DurableFunctionTestResult:
        """Create test result from execution history responses.

        Factory method for cloud runner that builds DurableFunctionTestResult
        from GetDurableExecution and GetDurableExecutionHistory API responses.
        """
        # Map status string to InvocationStatus enum
        try:
            status = InvocationStatus[execution_response.status]
        except KeyError:
            logger.warning(
                "Unknown status: %s, defaulting to FAILED", execution_response.status
            )
            status = InvocationStatus.FAILED

        # Convert Events to Operations - group by operation_id and merge
        try:
            svc_operations = events_to_operations(history_response.events)
        except Exception as e:
            logger.warning("Failed to convert events to operations: %s", e)
            svc_operations = []

        # Build operation tree (exclude EXECUTION type from top level)
        operations = []
        for svc_op in svc_operations:
            if svc_op.operation_type == OperationType.EXECUTION:
                continue
            if svc_op.parent_id is None:
                operations.append(create_operation(svc_op, svc_operations))

        return cls(
            status=status,
            operations=operations,
            result=execution_response.result,
            error=execution_response.error,
        )

    def get_operation_by_name(self, name: str) -> Operation:
        for operation in self.operations:
            if operation.name == name:
                return operation
        msg: str = f"Operation with name '{name}' not found"
        raise DurableFunctionsTestError(msg)

    def get_step(self, name: str) -> StepOperation:
        return cast("StepOperation", self.get_operation_by_name(name))

    def get_wait(self, name: str) -> WaitOperation:
        return cast("WaitOperation", self.get_operation_by_name(name))

    def get_context(self, name: str) -> ContextOperation:
        return cast("ContextOperation", self.get_operation_by_name(name))

    def get_callback(self, name: str) -> CallbackOperation:
        return cast("CallbackOperation", self.get_operation_by_name(name))

    def get_invoke(self, name: str) -> InvokeOperation:
        return cast("InvokeOperation", self.get_operation_by_name(name))

    def get_execution(self, name: str) -> ExecutionOperation:
        return cast("ExecutionOperation", self.get_operation_by_name(name))

    def get_deserialized_result(self, serdes: ExtendedTypeSerDes | None = None) -> Any:
        """Return the deserialized execution result."""
        return _deserialize_operation_payload(self.result, serdes)

    def get_all_operations(self) -> list[Operation]:
        """Recursively get all operations including nested ones."""
        all_ops = []
        stack = list(self.operations)
        while stack:
            op = stack.pop()
            all_ops.append(op)
            # Add child operations to stack (if they exist)
            if hasattr(op, "child_operations") and op.child_operations:
                stack.extend(op.child_operations)
        return all_ops


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
        self._checkpoint_processor = CheckpointProcessor(
            store=self._store, scheduler=self._scheduler
        )
        self._service_client = InMemoryServiceClient(self._checkpoint_processor)
        self._invoker = InProcessInvoker(handler, self._service_client)
        self._executor = Executor(
            store=self._store,
            scheduler=self._scheduler,
            invoker=self._invoker,
            checkpoint_processor=self._checkpoint_processor,
        )

        # Wire up observer pattern - CheckpointProcessor uses this to notify executor of state changes
        self._checkpoint_processor.add_execution_observer(self._executor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._scheduler.stop()

    def run(
        self,
    ) -> DurableFunctionTestResult:
        execution_arn = self.run_async()
        return self.wait_for_result(
            execution_arn=execution_arn, timeout=self._default_timeout
        )

    def send_callback_success(
        self, callback_id: str, result: bytes | None = None
    ) -> None:
        self._executor.send_callback_success(callback_id=callback_id, result=result)

    def send_callback_failure(
        self, callback_id: str, error: ErrorObject | None = None
    ) -> None:
        self._executor.send_callback_failure(callback_id=callback_id, error=error)

    def send_callback_heartbeat(self, callback_id: str) -> None:
        self._executor.send_callback_heartbeat(callback_id=callback_id)

    def run_async(
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

    def wait_for_result(
        self, execution_arn: str, timeout: int = 60
    ) -> DurableFunctionTestResult:
        # Block until completion
        completed = self._executor.wait_until_complete(execution_arn, timeout)

        if not completed:
            msg_timeout: str = "Execution did not complete within timeout"

            raise TimeoutError(msg_timeout)

        execution: Execution = self._store.load(execution_arn)
        return DurableFunctionTestResult.create(execution=execution)

    def wait_for_callback(
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

            # Wait before next poll
            time.sleep(self.poll_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        msg = f"Callback did not available within {timeout}s (elapsed: {elapsed:.1f}s."
        raise TimeoutError(msg)


def create_runner(
    *,
    mode: str,
    handler: Callable | None = None,
    function_name: str | None = None,
    region: str = "us-west-2",
    lambda_endpoint: str | None = None,
    poll_interval: float = 1.0,
    input: Any = None,  # noqa: A002
    timeout: int = 60,
) -> DurableFunctionLocalTestRunner | DurableFunctionCloudTestRunner:
    """Create a configured local or cloud durable function runner.

    Args:
        mode: Runner mode, either ``local`` or ``cloud``.
        handler: Durable handler to run locally. Required when ``mode='local'``.
        function_name: Qualified Lambda function name. Required when ``mode='cloud'``.
        region: AWS region for cloud mode.
        lambda_endpoint: Optional Lambda endpoint for cloud mode.
        poll_interval: Poll interval used by the underlying runner.
        input: Default input for ``run()`` and ``run_async()``.
        timeout: Default timeout for ``run()`` and ``run_async()``.

    Returns:
        A configured runner that can be used as a context manager.
    """
    if mode == "local":
        if handler is None:
            msg = "handler is required when mode='local'"
            raise InvalidParameterValueException(msg)
        runner = DurableFunctionLocalTestRunner(
            handler=handler,
            poll_interval=poll_interval,
            input=input,
            timeout=timeout,
        )
    elif mode == "cloud":
        if function_name is None:
            msg = "function_name is required when mode='cloud'"
            raise InvalidParameterValueException(msg)
        runner = DurableFunctionCloudTestRunner(
            function_name=function_name,
            region=region,
            lambda_endpoint=lambda_endpoint,
            poll_interval=poll_interval,
            input=input,
            timeout=timeout,
        )
    else:
        msg = f"Unsupported runner mode: {mode}"
        raise InvalidParameterValueException(msg)

    return runner


class WebRunner:
    """Web server runner for durable functions testing with HTTP API endpoints."""

    def __init__(self, config: WebRunnerConfig) -> None:
        """Initialize WebRunner with configuration.

        Args:
            config: WebRunnerConfig containing server and Lambda service settings
        """
        self._config = config
        self._server: WebServer | None = None
        self._scheduler: Scheduler | None = None
        self._store: ExecutionStore | None = None
        self._invoker: LambdaInvoker | None = None
        self._executor: Executor | None = None

    def __enter__(self) -> Self:
        """Context manager entry point.

        Returns:
            WebRunner: Self for use in with statement
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit point with cleanup.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        self.stop()

    def start(self) -> None:
        """Start the server and initialize all dependencies.

        Creates and configures all required components including scheduler,
        store, invoker, executor, and web server. It does not however start
        serving web requests, for that you need serve_forever.

        Raises:
            DurableFunctionsLocalRunnerError: If server is already started
        """
        if self._server is not None:
            msg = "Server is already running"
            raise DurableFunctionsLocalRunnerError(msg)

        # Create dependencies and server
        if self._config.store_type == StoreType.SQLITE:
            store_path = self._config.store_path
            self._store = SQLiteExecutionStore.create_and_initialize(store_path)
        elif self._config.store_type == StoreType.FILESYSTEM:
            store_path = self._config.store_path or ".durable_executions"
            self._store = FileSystemExecutionStore.create(store_path)
        else:
            self._store = InMemoryExecutionStore()
        self._scheduler = Scheduler()
        self._invoker = LambdaInvoker(self._create_boto3_client())

        # Create shared CheckpointProcessor
        checkpoint_processor = CheckpointProcessor(self._store, self._scheduler)

        # Create executor with all dependencies including checkpoint processor
        self._executor = Executor(
            store=self._store,
            scheduler=self._scheduler,
            invoker=self._invoker,
            checkpoint_processor=checkpoint_processor,
        )

        # Add executor as observer to the checkpoint processor
        checkpoint_processor.add_execution_observer(self._executor)

        # Start the scheduler
        self._scheduler.start()

        # Create web server with configuration and executor
        self._server = WebServer(
            config=self._config.web_service, executor=self._executor
        )

    def serve_forever(self) -> None:
        """Start serving HTTP requests indefinitely.

        Delegates to the underlying WebServer.serve_forever() method.
        This method blocks until the server is stopped.

        Raises:
            DurableFunctionsLocalRunnerError: If server has not been started
        """
        if self._server is None:
            msg = "Server not started"
            raise DurableFunctionsLocalRunnerError(msg)

        # This blocks until KeyboardInterrupt - let caller handle the exception
        self._server.serve_forever()

    def stop(self) -> None:
        """Stop the web server and cleanup resources.

        Gracefully shuts down the server, scheduler, and cleans up
        all allocated resources. Safe to call multiple times.
        Handles cleanup exceptions gracefully to ensure all resources
        are cleaned up even if some fail.
        """
        if self._server is not None:
            try:
                self._server.server_close()
            except Exception:
                # Log the exception but continue cleanup
                logger.exception("error closing web server")

            self._server = None

        if self._scheduler is not None:
            try:
                self._scheduler.stop()
            except Exception:
                logger.exception("error stopping scheduler")
            self._scheduler = None

        self._store = None
        self._invoker = None
        self._executor = None

    def _create_boto3_client(self) -> Any:
        """Create boto3 client for Lambda service.

        Creates a boto3 client with the local runner endpoint and region from configuration.

        Returns:
            Configured boto3 client for Lambda service

        Raises:
            Exception: If client creation fails - exceptions propagate naturally
                      for CLI to handle as general Exception
        """
        return create_lambda_client(
            endpoint_url=self._config.lambda_endpoint,
            region_name=self._config.local_runner_region,
        )


class DurableFunctionCloudTestRunner:
    """Test runner that executes durable functions against actual AWS Lambda backend.

    This runner invokes deployed Lambda functions and polls for execution completion,
    providing the same interface as DurableFunctionLocalTestRunner for seamless test
    compatibility between local and cloud modes.

    Example:
        >>> runner = DurableFunctionCloudTestRunner(
        ...     function_name="HelloWorld-Python-PR-123", region="us-west-2"
        ... )
        >>> with runner:
        ...     result = runner.run()
        >>> assert result.current_status == InvocationStatus.SUCCEEDED
    """

    def __init__(
        self,
        function_name: str,
        region: str = "us-west-2",
        lambda_endpoint: str | None = None,
        poll_interval: float = 1.0,
        input: Any = None,  # noqa: A002
        timeout: int = 60,
    ):
        """Initialize cloud test runner."""
        self.mode = "cloud"
        self.function_name = function_name
        self.region = region
        self.lambda_endpoint = lambda_endpoint
        self.poll_interval = poll_interval
        self._default_input = input
        self._default_timeout = timeout

        client_config = boto3.session.Config(parameter_validation=False)
        self.lambda_client = boto3.client(
            "lambda",
            endpoint_url=lambda_endpoint,
            region_name=region,
            config=client_config,
        )

    def __enter__(self) -> Self:
        """Return self for context manager compatibility with local runner."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close underlying resources when leaving a context manager block."""
        self.close()

    def close(self) -> None:
        """Close the underlying boto3 client when supported."""
        close = getattr(self.lambda_client, "close", None)
        if callable(close):
            close()

    def run(
        self,
    ) -> DurableFunctionTestResult:
        """Execute function on AWS Lambda and wait for completion."""
        logger.info(
            "Invoking Lambda function: %s (timeout: %ds)",
            self.function_name,
            self._default_timeout,
        )

        # JSON encode input
        payload = json.dumps(self._default_input)

        # Invoke Lambda function
        try:
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType="RequestResponse",
                Payload=payload,
            )
        except Exception as e:
            msg = f"Failed to invoke Lambda function {self.function_name}: {e}"
            raise DurableFunctionsTestError(msg) from e

        # Check HTTP status code, 200 for RequestResponse
        status_code = response.get("StatusCode")
        if status_code != 200:
            error_payload = response["Payload"].read().decode("utf-8")
            msg = f"Lambda invocation failed with status {status_code}: {error_payload}"
            raise DurableFunctionsTestError(msg)

        # Check for function errors, we want to return function error for testing purpose
        if "FunctionError" in response:
            error_payload = response["Payload"].read().decode("utf-8")
            logger.warning("Lambda function failed: %s", error_payload)

        result_payload = response["Payload"].read().decode("utf-8")
        logger.info(
            "Lambda invocation completed, response: %s",
            result_payload,
        )

        # Extract durable execution ARN from response headers
        # The InvocationResponse includes X-Amz-Durable-Execution-Arn header
        execution_arn = response.get("DurableExecutionArn")
        if not execution_arn:
            msg = (
                f"No DurableExecutionArn in response for function {self.function_name}"
            )
            raise DurableFunctionsTestError(msg)

        return self.wait_for_result(
            execution_arn=execution_arn, timeout=self._default_timeout
        )

    def run_async(
        self,
    ) -> str:
        """Execute function on AWS Lambda asynchronously"""
        logger.info(
            "Invoking Lambda function: %s (timeout: %ds)",
            self.function_name,
            self._default_timeout,
        )
        payload = json.dumps(self._default_input)
        try:
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType="Event",
                Payload=payload,
            )
        except Exception as e:
            msg = f"Failed to invoke Lambda function {self.function_name}: {e}"
            raise DurableFunctionsTestError(msg) from e

        # Check HTTP status code, 202 for Event
        status_code = response.get("StatusCode")
        if status_code != 202:
            error_payload = response["Payload"].read().decode("utf-8")
            msg = f"Lambda invocation failed with status {status_code}: {error_payload}"
            raise DurableFunctionsTestError(msg)

        return response.get("DurableExecutionArn")

    def send_callback_success(
        self, callback_id: str, result: bytes | None = None
    ) -> None:
        try:
            self.lambda_client.send_durable_execution_callback_success(
                CallbackId=callback_id, Result=result
            )
        except Exception as e:
            msg = f"Failed to send callback success for {self.function_name}, callback_id {callback_id}: {e}"
            raise DurableFunctionsTestError(msg) from e

    def send_callback_failure(
        self, callback_id: str, error: ErrorObject | None = None
    ) -> None:
        try:
            self.lambda_client.send_durable_execution_callback_failure(
                CallbackId=callback_id, Error=error.to_dict() if error else None
            )
        except Exception as e:
            msg = f"Failed to send callback failure for {self.function_name}, callback_id {callback_id}: {e}"
            raise DurableFunctionsTestError(msg) from e

    def send_callback_heartbeat(self, callback_id: str) -> None:
        try:
            self.lambda_client.send_durable_execution_callback_heartbeat(
                CallbackId=callback_id
            )
        except Exception as e:
            msg = f"Failed to send callback heartbeat for {self.function_name}, callback_id {callback_id}: {e}"
            raise DurableFunctionsTestError(msg) from e

    def _wait_for_completion(
        self, execution_arn: str, timeout: int
    ) -> GetDurableExecutionResponse:
        """Poll execution status until completion or timeout.

        Args:
            execution_arn: ARN of the durable execution
            timeout: Maximum seconds to wait

        Returns:
            GetDurableExecutionResponse with typed execution details

        Raises:
            TimeoutError: If execution doesn't complete within timeout
            DurableFunctionsTestError: If status check fails
        """
        start_time = time.time()
        last_status = None

        while time.time() - start_time < timeout:
            try:
                execution_dict = self.lambda_client.get_durable_execution(
                    DurableExecutionArn=execution_arn
                )
                execution = GetDurableExecutionResponse.from_dict(execution_dict)
            except Exception as e:
                msg = f"Failed to get execution status: {e}"
                raise DurableFunctionsTestError(msg) from e

            # Log status changes
            if execution.status != last_status:
                logger.info("Execution status: %s", execution.status)
                last_status = execution.status

            # Check if execution completed
            if execution.status == "SUCCEEDED":
                logger.info("Execution succeeded")
                return execution
            if execution.status == "FAILED":
                logger.warning("Execution failed")
                return execution
            if execution.status in ["TIMED_OUT", "ABORTED"]:
                logger.warning("Execution terminated: %s", execution.status)
                return execution

            # Wait before next poll
            time.sleep(self.poll_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        msg = (
            f"Execution did not complete within {timeout}s "
            f"(elapsed: {elapsed:.1f}s, last status: {last_status})"
        )
        raise TimeoutError(msg)

    def wait_for_result(
        self, execution_arn: str, timeout: int = 60
    ) -> DurableFunctionTestResult:
        # Poll for completion
        execution_response = self._wait_for_completion(execution_arn, timeout)

        try:
            history_response = self._fetch_execution_history(execution_arn)
        except Exception as e:
            msg = f"Failed to fetch execution history: {e}"
            raise DurableFunctionsTestError(msg) from e

        # Build test result from execution history
        return DurableFunctionTestResult.from_execution_history(
            execution_response, history_response
        )

    def wait_for_callback(
        self, execution_arn: str, name: str | None = None, timeout: int = 60
    ) -> str:
        """
        Wait for and retrieve a callback ID from a Step Functions execution.

        Polls the execution history at regular intervals until a callback ID is found
        or the timeout is reached.

        Args:
            execution_arn: Execution Arn
            name: Specific callback name, default to None
            timeout: Maximum time in seconds to wait for callback. Defaults to 60.

        Returns:
            str: The callback ID/token retrieved from the execution history

        Raises:
            TimeoutError: If callback is not found within the specified timeout period
            DurableFunctionsTestError: If there's an error fetching execution history
                (excluding retryable errors)
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                history_response = self._fetch_execution_history(execution_arn)
                callback_id = _get_callback_id_from_events(
                    events=history_response.events, name=name
                )
                if callback_id:
                    return callback_id
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                # retryable error, the execution may not start yet in async invoke situation
                if error_code in ["ResourceNotFoundException"]:
                    pass
                else:
                    msg = f"Failed to fetch execution history: {e}"
                    raise DurableFunctionsTestError(msg) from e
            except DurableFunctionsTestError as e:
                raise e
            except Exception as e:
                msg = f"Failed to fetch execution history: {e}"
                raise DurableFunctionsTestError(msg) from e

            # Wait before next poll
            time.sleep(self.poll_interval)

        # Timeout reached
        elapsed = time.time() - start_time
        msg = f"Callback did not available within {timeout}s (elapsed: {elapsed:.1f}s."
        raise TimeoutError(msg)

    def _fetch_execution_history(
        self, execution_arn: str
    ) -> GetDurableExecutionHistoryResponse:
        """Retrieve execution history from Lambda service.

        Args:
            execution_arn: ARN of the durable execution

        Returns:
            GetDurableExecutionHistoryResponse with typed Event objects

        Raises:
            ClientError: If lambda client encounter error
        """
        history_dict = self.lambda_client.get_durable_execution_history(
            DurableExecutionArn=execution_arn,
            IncludeExecutionData=True,
        )
        history_response = GetDurableExecutionHistoryResponse.from_dict(history_dict)

        logger.info("Retrieved %d events from history", len(history_response.events))

        return history_response

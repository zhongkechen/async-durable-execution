from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any, Callable

from ...core import (
    CheckpointError,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    DurableExecutionInvocationInput,
    DurableExecutionInvocationOutput,
    DurableServiceClient,
    ErrorObject,
    GetExecutionStateError,
    InitialExecutionState,
    Operation,
    OperationUpdate,
    StateOutput,
    _bind_service_client_to_handler,
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
        self.mode = "local"
        self.poll_interval = poll_interval
        self._default_input = input
        self._default_timeout = timeout
        self._function_name = function_name
        self._execution_name = execution_name
        self._account_id = account_id
        self._service_client = InMemoryServiceClient(scheduler=self._scheduler)
        self._invoker = InProcessInvoker(handler, self._service_client)
        self._executor = Executor(
            scheduler=self._scheduler,
            invoker=self._invoker,
            service_client=self._service_client,
        )

        self._service_client.bind_executor(self._executor)

    async def __aenter__(self) -> DurableFunctionLocalTestRunner:
        if scheduler := getattr(self, "_scheduler", None):
            scheduler.start()
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
        self._scheduler.start()
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
        completed = await self._executor.wait_until_complete(execution_arn, timeout)

        if not completed:
            msg_timeout: str = "Execution did not complete within timeout"

            raise TimeoutError(msg_timeout)

        execution: Execution = self._executor.get_execution(execution_arn)
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


class InMemoryServiceClient(DurableServiceClient):
    """An in-memory service client, that can replace the boto lambda service client."""

    def __init__(self, scheduler: Scheduler):
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
        if self._executor is None:
            unbound_msg = "Local executor is not bound to the service client."
            raise InvalidParameterValueException(unbound_msg)

        token: CheckpointToken = CheckpointToken.from_str(checkpoint_token)
        execution: Execution = self._executor.get_execution(token.execution_arn)

        if execution.is_complete or token.token_sequence != execution.token_sequence:
            msg: str = "Invalid checkpoint token"
            raise InvalidParameterValueException(msg)

        CheckpointValidator.validate_input(
            updates, execution, processors=self._transformer.processors
        )

        updated_operations, all_updates = self._transformer.process_updates(
            updates=updates,
            current_operations=execution.operations,
            runner=self._executor,
            execution_arn=token.execution_arn,
        )

        new_checkpoint_token = execution.get_new_checkpoint_token()
        execution.operations = updated_operations
        execution.updates.extend(all_updates)
        self._executor.set_execution(execution)

        return CheckpointOutput(
            checkpoint_token=new_checkpoint_token,
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=execution.get_navigable_operations(), next_marker=None
            ),
        )

    async def checkpoint(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        checkpoint_token: str | None,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # durable_execution_arn is not used in in-memory testing
        if not checkpoint_token:
            msg = "Cannot checkpoint without a checkpoint token."
            raise CheckpointError(msg)
        return self.process_checkpoint(checkpoint_token, updates, client_token)

    async def get_execution_state(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        checkpoint_token: str | None,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
        # durable_execution_arn is not used in in-memory testing
        if not checkpoint_token:
            msg = "Cannot get execution state without a checkpoint token."
            raise GetExecutionStateError(msg)

        if self._executor is None:
            msg = "Local executor is not bound to the service client."
            raise InvalidParameterValueException(msg)

        token: CheckpointToken = CheckpointToken.from_str(checkpoint_token)
        execution: Execution = self._executor.get_execution(token.execution_arn)

        # TODO: paging when size or max
        return StateOutput(
            operations=execution.get_navigable_operations(), next_marker=None
        )

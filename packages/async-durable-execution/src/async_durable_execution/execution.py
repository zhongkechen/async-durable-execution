from __future__ import annotations

import asyncio
import functools
import json
import logging
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from async_durable_execution.async_tools import (
    assert_async_callable,
    invoke_callable,
)
from async_durable_execution.context import DurableContext
from async_durable_execution.exceptions import (
    BotoClientError,
    CheckpointError,
    ExecutionError,
    InvocationError,
    SuspendExecution,
)
from async_durable_execution.models import (
    DurableExecutionInvocationOutput,
    ErrorObject,
    InvocationStatus,
    Operation,
    OperationUpdate,
)
from async_durable_execution.lambda_service import (
    DurableServiceClient,
    LambdaApiClient,
    ThreadedSyncLambdaClient,
)
from async_durable_execution.plugin import (
    DurableInstrumentationPlugin,
    PluginExecutor,
)
from async_durable_execution.state import ExecutionState, ReplayStatus


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, MutableMapping

    from async_durable_execution.types import LambdaContext


logger = logging.getLogger(__name__)

# 6MB in bytes, minus 50 bytes for envelope
LAMBDA_RESPONSE_SIZE_LIMIT = 6 * 1024 * 1024 - 50


@dataclass(frozen=True)
class InitialExecutionState:
    operations: list[Operation]
    next_marker: str

    @staticmethod
    def from_dict(input_dict: MutableMapping[str, Any]) -> InitialExecutionState:
        operations = []
        if input_operations := input_dict.get("Operations"):
            operations = [Operation.from_dict(op) for op in input_operations]
        return InitialExecutionState(
            operations=operations,
            next_marker=input_dict.get("NextMarker", ""),
        )

    @staticmethod
    def from_json_dict(input_dict: MutableMapping[str, Any]) -> InitialExecutionState:
        operations = []
        if input_operations := input_dict.get("Operations"):
            operations = [Operation.from_json_dict(op) for op in input_operations]
        return InitialExecutionState(
            operations=operations,
            next_marker=input_dict.get("NextMarker", ""),
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "Operations": [op.to_dict() for op in self.operations],
            "NextMarker": self.next_marker,
        }

    def to_json_dict(self) -> MutableMapping[str, Any]:
        return {
            "Operations": [op.to_json_dict() for op in self.operations],
            "NextMarker": self.next_marker,
        }


@dataclass(frozen=True)
class DurableExecutionInvocationInput:
    durable_execution_arn: str
    checkpoint_token: str
    initial_execution_state: InitialExecutionState

    @staticmethod
    def from_dict(
        input_dict: MutableMapping[str, Any],
    ) -> DurableExecutionInvocationInput:
        return DurableExecutionInvocationInput(
            durable_execution_arn=input_dict["DurableExecutionArn"],
            checkpoint_token=input_dict["CheckpointToken"],
            initial_execution_state=InitialExecutionState.from_dict(
                input_dict.get("InitialExecutionState", {})
            ),
        )

    @staticmethod
    def from_json_dict(
        input_dict: MutableMapping[str, Any],
    ) -> DurableExecutionInvocationInput:
        return DurableExecutionInvocationInput(
            durable_execution_arn=input_dict["DurableExecutionArn"],
            checkpoint_token=input_dict["CheckpointToken"],
            initial_execution_state=InitialExecutionState.from_json_dict(
                input_dict.get("InitialExecutionState", {})
            ),
        )

    def to_dict(self) -> MutableMapping[str, Any]:
        return {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
            "InitialExecutionState": self.initial_execution_state.to_dict(),
        }

    def to_json_dict(self) -> MutableMapping[str, Any]:
        return {
            "DurableExecutionArn": self.durable_execution_arn,
            "CheckpointToken": self.checkpoint_token,
            "InitialExecutionState": self.initial_execution_state.to_json_dict(),
        }


@dataclass(frozen=True)
class DurableExecutionInvocationInputWithClient(DurableExecutionInvocationInput):
    """Invocation input with Lambda boto client injected.

    This is useful for testing scenarios where you want to inject a mock client.
    """

    service_client: DurableServiceClient

    @staticmethod
    def from_durable_execution_invocation_input(
        invocation_input: DurableExecutionInvocationInput,
        service_client: DurableServiceClient,
    ):
        return DurableExecutionInvocationInputWithClient(
            durable_execution_arn=invocation_input.durable_execution_arn,
            checkpoint_token=invocation_input.checkpoint_token,
            initial_execution_state=invocation_input.initial_execution_state,
            service_client=service_client,
        )


def durable_execution(
    func: Callable[[Any, DurableContext], Awaitable[Any]] | None = None,
    *,
    boto3_client: LambdaApiClient | None = None,
    plugins: list[DurableInstrumentationPlugin] | None = None,
) -> Callable[[Any, LambdaContext], Any]:
    """
    Decorator to create a durable execution handler.

    Args:
        func: The user function to decorate
        boto3_client: Optional boto3 Lambda client to use
        plugins: Optional list of plugins to use (EXPERIMENTAL: This
            feature has known issues and this parameter may change or be removed.)
    """
    # Decorator called with parameters
    if func is None:
        logger.debug("Decorator called with parameters")
        return functools.partial(
            durable_execution, boto3_client=boto3_client, plugins=plugins
        )

    logger.debug("Starting durable execution handler...")
    assert_async_callable(func, label="func")

    if plugins:
        warnings.warn(
            "The 'plugins' parameter is provisional and may be altered or removed.",
            category=FutureWarning,
            stacklevel=2,  # point the warning to the caller of durable_execution
        )

    plugin_executor = PluginExecutor(plugins)

    async def _wrapper_with_plugins(
        event: Any, context: LambdaContext
    ) -> MutableMapping[str, Any]:
        with plugin_executor.run():
            try:
                output = await _wrapper_async(event, context)
                await plugin_executor.on_invocation_end(
                    output=DurableExecutionInvocationOutput.from_dict(output),
                )
                return output
            except Exception as e:
                await plugin_executor.on_invocation_end(
                    output=DurableExecutionInvocationOutput.create_retry(
                        ErrorObject.from_exception(e)
                    ),
                )
                raise

    def wrapper(event: Any, context: LambdaContext) -> MutableMapping[str, Any]:
        return asyncio.run(_wrapper_with_plugins(event, context))

    wrapper._async_handler = _wrapper_with_plugins  # type: ignore[attr-defined]  # noqa: SLF001

    async def _wrapper_async(
        event: Any, context: LambdaContext
    ) -> MutableMapping[str, Any]:
        invocation_input: DurableExecutionInvocationInput
        service_client: DurableServiceClient

        # event likely only to be DurableExecutionInvocationInputWithClient when directly injected by test framework
        if isinstance(event, DurableExecutionInvocationInputWithClient):
            logger.debug("durableExecutionArn: %s", event.durable_execution_arn)
            invocation_input = event
            service_client = invocation_input.service_client
        else:
            try:
                logger.debug(
                    "durableExecutionArn: %s", event.get("DurableExecutionArn")
                )
                invocation_input = DurableExecutionInvocationInput.from_json_dict(event)
            except (KeyError, TypeError, AttributeError) as e:
                msg = (
                    "Unexpected payload provided to start the durable execution. "
                    "Check your resource configurations to confirm the durability is set."
                )
                raise ExecutionError(msg) from e

            # Use custom client if provided, otherwise initialize from environment
            service_client = (
                ThreadedSyncLambdaClient(client=boto3_client)
                if boto3_client is not None
                else ThreadedSyncLambdaClient.initialize_client()
            )

        execution_state: ExecutionState = ExecutionState(
            durable_execution_arn=invocation_input.durable_execution_arn,
            initial_checkpoint_token=invocation_input.checkpoint_token,
            operations={},
            service_client=service_client,
            replay_status=ReplayStatus.NEW,
            plugin_executor=plugin_executor,
        )

        try:
            await execution_state._fetch_paginated_operations_async(
                invocation_input.initial_execution_state.operations,
                invocation_input.checkpoint_token,
                invocation_input.initial_execution_state.next_marker,
            )
        except BotoClientError as e:
            # Non-retryable Durable API errors (e.g., customer configuration issues,
            # 4xx client errors) will never succeed on retry — fail the execution immediately.
            if not e.is_retryable():
                logger.exception(
                    "Non-retryable Durable API error during initial state fetch. Must fail execution "
                    "without retry.",
                    extra=e.build_logger_extras(),
                )
                return DurableExecutionInvocationOutput(
                    status=InvocationStatus.FAILED,
                    error=ErrorObject.from_exception(e),
                ).to_dict()
            raise

        execution_state.mark_replaying_if_prior_operations_exist()

        raw_input_payload: str | None = execution_state.get_input_payload()

        # Python RIC LambdaMarshaller just uses standard json deserialization for event
        # https://github.com/aws/aws-lambda-python-runtime-interface-client/blob/main/awslambdaric/lambda_runtime_marshaller.py#L46
        input_event: MutableMapping[str, Any] = {}
        if raw_input_payload and raw_input_payload.strip():
            try:
                input_event = json.loads(raw_input_payload)
            except json.JSONDecodeError:
                logger.exception(
                    "Failed to parse input payload as JSON: payload: %r",
                    raw_input_payload,
                )
                raise

        durable_context: DurableContext = DurableContext.from_lambda_context(
            state=execution_state, lambda_context=context
        )

        try:
            execution_operation = execution_state.get_execution_operation()
            if execution_operation is None:
                msg = "Execution state is missing the root execution operation."
                raise RuntimeError(msg)
            # execute the plugins
            await plugin_executor.on_invocation_start(
                execution_arn=invocation_input.durable_execution_arn,
                lambda_context=context,
                execution_start_time=execution_operation.start_timestamp,
                is_first_invocation=not execution_state.is_replaying(),
            )
            execution_state.start_checkpointing()

            logger.debug(
                "%s entering user-space...", invocation_input.durable_execution_arn
            )

            logger.debug(
                "%s waiting for user code completion...",
                invocation_input.durable_execution_arn,
            )

            try:
                result = await invoke_callable(func, input_event, durable_context)

                # done with userland
                logger.debug(
                    "%s exiting user-space...",
                    invocation_input.durable_execution_arn,
                )
                serialized_result = json.dumps(result)
                # large response handling here. Remember if checkpointing to complete, NOT to include
                # payload in response
                if (
                    serialized_result
                    and len(serialized_result) > LAMBDA_RESPONSE_SIZE_LIMIT
                ):
                    logger.debug(
                        "Response size (%s bytes) exceeds Lambda limit (%s) bytes). Checkpointing result.",
                        len(serialized_result),
                        LAMBDA_RESPONSE_SIZE_LIMIT,
                    )
                    success_operation = OperationUpdate.create_execution_succeed(
                        payload=serialized_result
                    )
                    # Checkpoint large result with blocking (is_sync=True, default).
                    # Must ensure the result is persisted before returning to Lambda.
                    # Large results exceed Lambda response limits and must be stored durably
                    # before the execution completes.
                    try:
                        await execution_state.create_checkpoint(
                            success_operation, is_sync=True
                        )
                    except CheckpointError as e:
                        return handle_checkpoint_error(e).to_dict()
                    return DurableExecutionInvocationOutput.create_succeeded(
                        result=""
                    ).to_dict()

                return DurableExecutionInvocationOutput.create_succeeded(
                    result=serialized_result
                ).to_dict()

            except SuspendExecution:
                logger.debug("Suspending execution...")
                return DurableExecutionInvocationOutput(
                    status=InvocationStatus.PENDING
                ).to_dict()

            except CheckpointError as e:
                # Checkpoint system is broken - stop background thread and exit immediately
                logger.exception(
                    "Checkpoint system failed",
                    extra=e.build_logger_extras(),
                )
                return handle_checkpoint_error(e).to_dict()
            except InvocationError as e:
                # Non-retryable Durable API errors (e.g., customer configuration issues,
                # 4xx client errors) will never succeed on retry — fail the execution immediately.
                if not e.is_retryable():
                    logger.exception(
                        "Non-retryable Durable API error. Must fail execution without retry.",
                        extra=e.build_logger_extras(),  # type: ignore[attr-defined]
                    )
                    return DurableExecutionInvocationOutput(
                        status=InvocationStatus.FAILED,
                        error=ErrorObject.from_exception(e),
                    ).to_dict()
                logger.exception("Invocation error. Must terminate.")
                # Throw the error to trigger Lambda retry
                raise
            except ExecutionError as e:
                logger.exception("Execution error. Must fail execution without retry.")
                return DurableExecutionInvocationOutput(
                    status=InvocationStatus.FAILED,
                    error=ErrorObject.from_exception(e),
                ).to_dict()
            except Exception as e:
                # all user-space errors go here
                logger.exception("Execution failed")

                result = DurableExecutionInvocationOutput(
                    status=InvocationStatus.FAILED, error=ErrorObject.from_exception(e)
                ).to_dict()

                serialized_result = json.dumps(result)

                if (
                    serialized_result
                    and len(serialized_result) > LAMBDA_RESPONSE_SIZE_LIMIT
                ):
                    logger.debug(
                        "Response size (%s bytes) exceeds Lambda limit (%s) bytes). Checkpointing result.",
                        len(serialized_result),
                        LAMBDA_RESPONSE_SIZE_LIMIT,
                    )
                    failed_operation = OperationUpdate.create_execution_fail(
                        error=ErrorObject.from_exception(e)
                    )

                    # Checkpoint large result with blocking (is_sync=True, default).
                    # Must ensure the result is persisted before returning to Lambda.
                    # Large results exceed Lambda response limits and must be stored durably
                    # before the execution completes.
                    try:
                        await execution_state.create_checkpoint(
                            failed_operation, is_sync=True
                        )
                    except CheckpointError as e:
                        return handle_checkpoint_error(e).to_dict()
                    return DurableExecutionInvocationOutput(
                        status=InvocationStatus.FAILED
                    ).to_dict()

                return result
        finally:
            await execution_state.aclose()

    return wrapper


def handle_checkpoint_error(error: CheckpointError) -> DurableExecutionInvocationOutput:
    if error.is_retryable():
        raise error from None  # Terminate Lambda immediately and have it be retried
    return DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=ErrorObject.from_exception(error)
    )

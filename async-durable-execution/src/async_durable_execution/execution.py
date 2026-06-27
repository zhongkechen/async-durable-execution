from __future__ import annotations

import asyncio
import functools
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

from .context import invoke_user_callable
from .primitive.child import DurableContext
from .exceptions import (
    CheckpointError,
    ExecutionError,
    InvocationError,
    SuspendExecution,
)
from .models import (
    DurableExecutionInvocationOutput,
    ErrorObject,
    InvocationStatus,
    Operation,
    OperationUpdate,
    OperationIdentifier,
    SerializableModel,
)
from .client import (
    AsyncLambdaClient,
    ThreadedSyncLambdaClient,
    create_default_client,
    lambda_api_client_is_async,
)
from .logger import configure_durable_logger
from .plugin import (
    DurableInstrumentationPlugin,
    PluginExecutor,
)
from .state import ExecutionState


if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from .types import (
        AsyncLambdaApiClient,
        DurableServiceClient,
        LambdaContext,
        LambdaApiClient,
    )

configure_durable_logger(logging.getLogger())
logger = logging.getLogger(__name__)

# 6MB in bytes, minus 50 bytes for envelope
LAMBDA_RESPONSE_SIZE_LIMIT = 6 * 1024 * 1024 - 50

T = TypeVar("T")
Params = ParamSpec("Params")


def durable_callable(
    func: Callable[Params, Awaitable[T]],
) -> Callable[Params, Callable[[], Awaitable[T]]]:
    """Wrap an async function so calling it returns a zero-argument durable callable.

    The returned callable can be passed to durable operations such as `step()`
    and `run_in_child_context()`, keeping durable operation creation explicit
    while avoiding manual `functools.partial(...)` wrapping at the callsite.

    Class and static methods are supported with either decorator order:
    `@classmethod`/`@staticmethod` may appear above or below `@durable_callable`.
    """
    if isinstance(func, classmethod):
        return classmethod(durable_callable(func.__func__))  # type: ignore[return-value]
    if isinstance(func, staticmethod):
        return staticmethod(durable_callable(func.__func__))  # type: ignore[return-value]

    @functools.wraps(func)
    def wrapper(
        *args: Params.args, **kwargs: Params.kwargs
    ) -> Callable[[], Awaitable[T]]:
        bound = functools.partial(func, *args, **kwargs)
        setattr(bound, "__name__", func.__name__)
        return bound

    return wrapper


@dataclass(frozen=True)
class InitialExecutionState(SerializableModel):
    """Initial page of operation history included with an invocation event."""

    operations: list[Operation] = field(
        default_factory=list,
        metadata={"alias": "Operations"},
    )
    next_marker: str = field(default="", metadata={"alias": "NextMarker"})


@dataclass(frozen=True)
class DurableExecutionInvocationInput(SerializableModel):
    """Event payload delivered to a durable Lambda invocation."""

    durable_execution_arn: str = field(metadata={"alias": "DurableExecutionArn"})
    checkpoint_token: str = field(metadata={"alias": "CheckpointToken"})
    initial_execution_state: InitialExecutionState = field(
        default_factory=InitialExecutionState,
        metadata={"alias": "InitialExecutionState"},
    )


def _bind_service_client_to_handler(
    handler: Callable[[Any, LambdaContext], Any],
    service_client: DurableServiceClient,
) -> Callable[[Any, LambdaContext], Any]:
    """Recreate a durable handler with a specific service client bound."""

    handler_attrs = getattr(handler, "__dict__", {})
    original_handler = handler_attrs.get("_durable_execution_original")
    if original_handler is None:
        return handler

    plugins = handler_attrs.get("_durable_execution_plugins")
    boto3_client = handler_attrs.get("_durable_execution_boto3_client")
    return durable_execution(
        original_handler,
        boto3_client=boto3_client,
        service_client=service_client,
        plugins=plugins,
    )


@dataclass(frozen=True)
class DurableConfig:
    boto3_client: LambdaApiClient | AsyncLambdaApiClient | None = None
    service_client: DurableServiceClient | None = None
    plugins: list[DurableInstrumentationPlugin] | None = None


def durable_execution(
    func: Callable[..., Awaitable[Any]] | None = None,
    /,
    **kwargs,
) -> Callable[[Any, LambdaContext], Any]:
    """
    Decorator to create a durable execution handler.

    Args:
        func: The user function to decorate
        boto3_client: Optional sync or async Lambda API client to use
        service_client: Optional durable service client to use. Intended for
            testing and local execution tooling.
        plugins: Optional list of plugins to use (EXPERIMENTAL: This
            feature has known issues and this parameter may change or be removed.)
    """
    # Decorator called with parameters
    if func is None:
        logger.debug("Decorator called with parameters")
        return functools.partial(
            durable_execution,
            **kwargs,
        )
    config = DurableConfig(**kwargs)
    logger.debug("Starting durable execution handler...")
    plugin_executor = PluginExecutor(config.plugins)

    # Use the explicitly provided durable client when present. Otherwise, delay
    # Lambda API client construction until invocation so importing decorated handlers
    # does not require AWS environment configuration.
    active_service_client = config.service_client

    def get_active_service_client() -> DurableServiceClient:
        nonlocal active_service_client
        if active_service_client is None:
            if config.boto3_client is not None:
                if lambda_api_client_is_async(config.boto3_client):
                    active_service_client = AsyncLambdaClient(
                        cast("AsyncLambdaApiClient", config.boto3_client)
                    )
                else:
                    active_service_client = ThreadedSyncLambdaClient(
                        client=cast("LambdaApiClient", config.boto3_client)
                    )
            else:
                lambda_client = create_default_client()
                if lambda_api_client_is_async(lambda_client):
                    active_service_client = AsyncLambdaClient(
                        cast("AsyncLambdaApiClient", lambda_client)
                    )
                else:
                    active_service_client = ThreadedSyncLambdaClient(
                        client=cast("LambdaApiClient", lambda_client)
                    )
        return active_service_client

    async def async_wrapper(
        event: Any, context: LambdaContext
    ) -> MutableMapping[str, Any]:
        return (
            await _wrapper_with_plugins(
                func, event, context, plugin_executor, get_active_service_client()
            )
        ).to_dict()

    @functools.wraps(func)
    def wrapper(event: Any, context: LambdaContext) -> MutableMapping[str, Any]:
        return asyncio.run(async_wrapper(event, context))

    wrapper._async_handler = async_wrapper  # type: ignore[attr-defined]  # noqa: SLF001
    wrapper._durable_execution_original = func  # type: ignore[attr-defined]  # noqa: SLF001
    wrapper._durable_execution_boto3_client = config.boto3_client  # type: ignore[attr-defined]  # noqa: SLF001
    wrapper._durable_execution_plugins = config.plugins  # type: ignore[attr-defined]  # noqa: SLF001

    return wrapper


async def _wrapper_with_plugins(
    user_func: Callable[[Any], Any],
    event: Any,
    context: LambdaContext,
    plugin_executor: PluginExecutor,
    service_client: DurableServiceClient,
) -> DurableExecutionInvocationOutput:
    with plugin_executor.run():
        try:
            output = await _wrapper_async(
                user_func, event, context, plugin_executor, service_client
            )
            await plugin_executor.on_invocation_end(
                output=output,
            )
            return output
        except Exception as e:
            await plugin_executor.on_invocation_end(
                output=DurableExecutionInvocationOutput.create_retry(
                    ErrorObject.from_exception(e)
                ),
            )
            raise


def deserialize_input(event: Any) -> DurableExecutionInvocationInput:
    try:
        logger.debug("durableExecutionArn: %s", event.get("DurableExecutionArn"))
        return DurableExecutionInvocationInput.from_json_dict(event)
    except (KeyError, TypeError, AttributeError) as e:
        msg = (
            "Unexpected payload provided to start the durable execution. "
            "Check your resource configurations to confirm the durability is set."
        )
        raise ExecutionError(msg) from e


async def _wrapper_async(
    user_func: Callable[[Any], Any],
    event: Any,
    context: LambdaContext,
    plugin_executor: PluginExecutor,
    service_client: DurableServiceClient,
) -> DurableExecutionInvocationOutput:
    invocation_input = deserialize_input(event)
    execution_state: ExecutionState = ExecutionState(
        durable_execution_arn=invocation_input.durable_execution_arn,
        initial_checkpoint_token=invocation_input.checkpoint_token,
        service_client=service_client,
        plugin_executor=plugin_executor,
        lambda_context=context,
    )

    try:
        await execution_state.initialize(invocation_input)

        input_event = execution_state.get_input_event()

        root_context = DurableContext(
            execution_state=execution_state,
            operation_identifier=OperationIdentifier.create_execution_op(),
            replaying=execution_state.has_prior_operations(),
        )

        execution_operation = execution_state.get_execution_operation()
        if execution_operation is None:
            msg = "Execution state is missing the root execution operation."
            raise RuntimeError(msg)
        # execute the plugins
        await plugin_executor.on_invocation_start(
            execution_arn=invocation_input.durable_execution_arn,
            lambda_context=context,
            execution_start_time=execution_operation.start_timestamp,
            is_first_invocation=not execution_state.has_prior_operations(),
        )
        execution_state.start_checkpointing()

        logger.debug("execution arn: %s", invocation_input.durable_execution_arn)

        result = await invoke_user_callable(
            root_context,
            user_func,
            input_event,
        )
        return await handle_user_function_result(execution_state, result)

    except SuspendExecution:
        logger.debug("Suspending execution...")
        return DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
    except Exception as e:
        return await handle_user_function_exception(execution_state, e)
    finally:
        await execution_state.aclose()


async def handle_user_function_result(
    execution_state, result
) -> DurableExecutionInvocationOutput:
    # done with userland
    serialized_result = json.dumps(result)
    # large response handling here. Remember if checkpointing to complete, NOT to include
    # payload in response
    if serialized_result and len(serialized_result) > LAMBDA_RESPONSE_SIZE_LIMIT:
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
        await execution_state.create_checkpoint(success_operation, is_sync=True)

        return DurableExecutionInvocationOutput.create_succeeded(result="")
    return DurableExecutionInvocationOutput.create_succeeded(result=serialized_result)


async def handle_user_function_exception(
    execution_state, e: Exception
) -> DurableExecutionInvocationOutput:
    if isinstance(e, CheckpointError):
        return handle_checkpoint_error(e)
    if isinstance(e, InvocationError):
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
            )
        logger.exception("Invocation error. Must terminate.")
        # Throw the error to trigger Lambda retry
        raise
    if isinstance(e, ExecutionError):
        logger.exception("Execution error. Must fail execution without retry.")
        return DurableExecutionInvocationOutput(
            status=InvocationStatus.FAILED,
            error=ErrorObject.from_exception(e),
        )

    # all user-space errors go here
    logger.exception("Execution failed")

    result = DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=ErrorObject.from_exception(e)
    )

    serialized_result = json.dumps(result.to_dict())

    if serialized_result and len(serialized_result) > LAMBDA_RESPONSE_SIZE_LIMIT:
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
            await execution_state.create_checkpoint(failed_operation, is_sync=True)
        except CheckpointError as e:
            return handle_checkpoint_error(e)
        return DurableExecutionInvocationOutput(status=InvocationStatus.FAILED)
    return result


def handle_checkpoint_error(error: CheckpointError) -> DurableExecutionInvocationOutput:
    """Convert checkpoint failures into a final result or retry trigger."""
    # Checkpoint system is broken - stop background thread and exit immediately
    logger.exception("Checkpoint system failed", extra=error.build_logger_extras())
    if error.is_retryable():
        raise error from None  # Terminate Lambda immediately and have it be retried
    return DurableExecutionInvocationOutput(
        status=InvocationStatus.FAILED, error=ErrorObject.from_exception(error)
    )

from __future__ import annotations

import asyncio
import functools
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

from .context import bind_current_context
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
from .state import ExecutionState


if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from .client import AsyncLambdaApiClient, DurableServiceClient, LambdaApiClient
    from .models import LambdaContext

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
        return classmethod(durable_callable(func.__func__))
    if isinstance(func, staticmethod):
        return staticmethod(durable_callable(func.__func__))

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

    boto3_client = handler_attrs.get("_durable_execution_boto3_client")
    return durable_execution(
        original_handler,
        boto3_client=boto3_client,
        service_client=service_client,
    )


@dataclass(frozen=True)
class DurableConfig:
    boto3_client: LambdaApiClient | AsyncLambdaApiClient | None = None
    service_client: DurableServiceClient | None = None


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

    # Use the explicitly provided durable client when present. Otherwise, delay
    # Lambda API client construction until invocation so importing decorated handlers
    # does not require AWS environment configuration.
    active_service_client = config.service_client
    active_service_client_loop: asyncio.AbstractEventLoop | None = None
    handler_loop: asyncio.AbstractEventLoop | None = None

    def get_or_create_handler_loop() -> asyncio.AbstractEventLoop:
        nonlocal handler_loop
        if handler_loop is None or handler_loop.is_closed():
            handler_loop = asyncio.new_event_loop()
        return handler_loop

    async def get_active_service_client() -> DurableServiceClient:
        nonlocal active_service_client, active_service_client_loop
        current_loop = asyncio.get_running_loop()
        if active_service_client is not None:
            if (
                active_service_client_loop is None
                or active_service_client_loop is current_loop
            ):
                return active_service_client

            # The SDK-owned async client is bound to the loop that first used it.
            # If test code calls the async handler on another loop, rebuild it there.
            active_service_client = None
            active_service_client_loop = None

        if config.boto3_client is not None:
            if lambda_api_client_is_async(config.boto3_client):
                active_service_client = AsyncLambdaClient(
                    cast("AsyncLambdaApiClient", config.boto3_client)
                )
            else:
                active_service_client = ThreadedSyncLambdaClient(
                    client=cast("LambdaApiClient", config.boto3_client)
                )
            return active_service_client

        lambda_client = create_default_client()
        if lambda_api_client_is_async(lambda_client):
            active_service_client = AsyncLambdaClient(
                cast("AsyncLambdaApiClient", lambda_client)
            )
            active_service_client_loop = current_loop
            return active_service_client

        active_service_client = ThreadedSyncLambdaClient(
            client=cast("LambdaApiClient", lambda_client)
        )
        return active_service_client

    async def async_wrapper(
        event: Any, context: LambdaContext
    ) -> MutableMapping[str, Any]:
        service_client = await get_active_service_client()
        return (await _wrapper_async(func, event, context, service_client)).to_dict()

    @functools.wraps(func)
    def wrapper(event: Any, context: LambdaContext) -> MutableMapping[str, Any]:
        return _run_on_event_loop(
            get_or_create_handler_loop(), async_wrapper, event, context
        )

    setattr(wrapper, "_async_handler", async_wrapper)
    setattr(wrapper, "_durable_execution_original", func)
    setattr(wrapper, "_durable_execution_boto3_client", config.boto3_client)

    return wrapper


def _run_on_event_loop(
    loop: asyncio.AbstractEventLoop,
    async_func: Callable[..., Awaitable[MutableMapping[str, Any]]],
    *args: Any,
) -> MutableMapping[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        msg = (
            "durable_execution sync handlers cannot be called from a running "
            "event loop. Use the handler's _async_handler attribute instead."
        )
        raise RuntimeError(msg)

    previous_loop: asyncio.AbstractEventLoop | None = None
    had_previous_loop = True
    try:
        previous_loop = asyncio.get_event_loop()
    except RuntimeError:
        had_previous_loop = False

    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(async_func(*args))
    finally:
        asyncio.set_event_loop(previous_loop if had_previous_loop else None)


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
    service_client: DurableServiceClient,
) -> DurableExecutionInvocationOutput:
    invocation_input = deserialize_input(event)
    execution_state: ExecutionState = ExecutionState(
        durable_execution_arn=invocation_input.durable_execution_arn,
        initial_checkpoint_token=invocation_input.checkpoint_token,
        service_client=service_client,
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

        if execution_state.get_execution_operation() is None:
            msg = "Execution state is missing the root execution operation."
            raise RuntimeError(msg)
        execution_state.start_checkpointing()

        logger.debug("execution arn: %s", invocation_input.durable_execution_arn)

        with bind_current_context(root_context):
            result = await user_func(input_event)
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
                extra=e.build_logger_extras(),
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

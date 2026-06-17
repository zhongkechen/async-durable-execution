"""Implementation for run_in_child_context."""

from __future__ import annotations

import functools
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar, cast, ParamSpec

from .base import (
    CheckResult,
    OperationExecutor,
    OperationContext,
)
from ..async_tools import assert_async_callable, invoke_user_callable
from ..async_tools import get_callable_name
from ..config import ChildConfig
from ..context import OperationIdGenerator
from ..context import get_current_context
from ..exceptions import (
    InvocationError,
    SuspendExecution,
)
from ..models import (
    ContextOptions,
    ErrorObject,
    OperationIdentifier,
    OperationSubType,
    OperationUpdate,
)
from ..serdes import deserialize, serialize
from ..types import LambdaContext

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ..state import (
        CheckpointedResult,
        ExecutionState,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T")
Params = ParamSpec("Params")

# Checkpoint size limit in bytes (256KB)
CHECKPOINT_SIZE_LIMIT = 256 * 1024


class ChildOperationExecutor(OperationExecutor[T]):
    """Executor for child context operations.

    Checks operation status after creating START checkpoints to handle operations
    that complete synchronously, avoiding unnecessary execution or suspension.

    Handles large payload scenarios with ReplayChildren mode.
    """

    def __init__(
        self,
        func: Callable[[], Awaitable[T]],
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        config: ChildConfig,
    ):
        """Initialize the child operation executor.

        Args:
            func: The child context function to execute
            state: The execution state
            operation_identifier: The operation identifier
            config: The child configuration
        """
        self.func = func
        self.state = state
        self.operation_identifier = operation_identifier
        self.config = config
        self.is_virtual: bool = config.is_virtual
        self.sub_type = config.sub_type or OperationSubType.RUN_IN_CHILD_CONTEXT

    async def check_result_status(self) -> CheckResult[T]:
        """Check operation status and create START checkpoint if needed.

        Called twice by process() when creating synchronous checkpoints: once before
        and once after, to detect if the operation completed immediately.

        Returns:
            CheckResult indicating the next action to take

        Raises:
            CallableRuntimeError: For FAILED operations
        """
        operation_id = self.operation_identifier.require_operation_id()
        checkpointed_result: CheckpointedResult = self.state.get_checkpoint_result(
            operation_id
        )

        # Terminal success without replay_children - deserialize and return
        if (
            checkpointed_result.is_succeeded()
            and not checkpointed_result.is_replay_children()
        ):
            logger.debug(
                "Child context already completed, skipping execution for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            if checkpointed_result.result is None:
                return CheckResult.create_completed(None)  # type: ignore

            result: T = deserialize(
                serdes=self.config.serdes,
                data=checkpointed_result.result,
                operation_id=operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )
            return CheckResult.create_completed(result)

        # Terminal success with replay_children - re-execute
        if (
            checkpointed_result.is_succeeded()
            and checkpointed_result.is_replay_children()
        ):
            return CheckResult.create_is_ready_to_execute(checkpointed_result)

        # Terminal failure
        if checkpointed_result.is_failed():
            checkpointed_result.raise_callable_error()

        # Create START checkpoint if not exists
        if not checkpointed_result.is_existent() and not self.is_virtual:
            start_operation: OperationUpdate = OperationUpdate.create_context_start(
                identifier=self.operation_identifier,
                sub_type=self.sub_type,
            )
            # Checkpoint child context START with non-blocking (is_sync=False).
            # This is a fire-and-forget operation for performance - we don't need to wait for
            # persistence before executing the child context. The START checkpoint is purely
            # for observability and tracking the operation hierarchy.
            await self.state._create_checkpoint_async(
                operation_update=start_operation, is_sync=False
            )

        # Ready to execute (checkpoint exists or was just created)
        return CheckResult.create_is_ready_to_execute(checkpointed_result)

    async def execute(self, checkpointed_result: CheckpointedResult) -> T:
        """Execute child context function with error handling and large payload support.

        Args:
            checkpointed_result: The checkpoint data containing operation state

        Returns:
            The result of executing the child context function

        Raises:
            SuspendExecution: Re-raised without checkpointing
            InvocationError: Re-raised after checkpointing FAIL
            CallableRuntimeError: Raised for other exceptions after checkpointing FAIL
        """
        logger.debug(
            "▶️ Executing child context for id: %s, name: %s",
            self.operation_identifier.operation_id,
            self.operation_identifier.name,
        )
        try:
            operation_id = self.operation_identifier.require_operation_id()
            # TODO: fix attempt (checkpointed_result.is_existent is always True)
            wrapped_user_func = self.state.wrap_user_function(
                self.func,
                self.operation_identifier,
                checkpointed_result.is_replay_children(),
                attempt=None if checkpointed_result.is_existent() else 1,
            )
            raw_result: T = await wrapped_user_func()

            if self.is_virtual:
                logger.debug(
                    "Virtual context: Exiting child context without creating another checkpoint. id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                )
                return raw_result

            # If in replay_children mode, return without checkpointing
            if checkpointed_result.is_replay_children():
                logger.debug(
                    "ReplayChildren mode: Executed child context again on replay due to large payload. Exiting child context without creating another checkpoint. id: %s, name: %s",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                )
                return raw_result

            # Serialize result
            serialized_result: str = serialize(
                serdes=self.config.serdes,
                value=raw_result,
                operation_id=operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )

            # Check payload size and use ReplayChildren mode if needed
            # Summary Generator Logic:
            # When the serialized result exceeds 256KB, we use ReplayChildren mode to avoid
            # checkpointing large payloads. Instead, we checkpoint a compact summary and mark
            # the operation for replay. This matches the TypeScript implementation behavior.
            #
            # See TypeScript reference:
            # - aws-durable-execution-sdk-js/src/handlers/run-in-child-context-handler/run-in-child-context-handler.ts (lines ~200-220)
            #
            # The summary generator creates a JSON summary with metadata (type, counts, status)
            # instead of the full BatchResult. During replay, the child context is re-executed
            # to reconstruct the full result rather than deserializing from the checkpoint.
            replay_children: bool = False
            if len(serialized_result) > CHECKPOINT_SIZE_LIMIT:
                logger.debug(
                    "Large payload detected, using ReplayChildren mode: id: %s, name: %s, payload_size: %d, limit: %d",
                    self.operation_identifier.operation_id,
                    self.operation_identifier.name,
                    len(serialized_result),
                    CHECKPOINT_SIZE_LIMIT,
                )
                replay_children = True
                # Use summary generator if provided, otherwise use empty string (matches TypeScript)
                serialized_result = (
                    self.config.summary_generator(raw_result)
                    if self.config.summary_generator
                    else ""
                )

            # Checkpoint SUCCEED
            success_operation: OperationUpdate = OperationUpdate.create_context_succeed(
                identifier=self.operation_identifier,
                payload=serialized_result,
                sub_type=self.sub_type,
                context_options=ContextOptions(replay_children=replay_children),
            )
            # Checkpoint child context SUCCEED with blocking (is_sync=True, default).
            # Must ensure the child context result is persisted before returning to the parent.
            # This guarantees the result is durable and child operations won't be re-executed on replay
            # (unless replay_children=True for large payloads).
            await self.state._create_checkpoint_async(
                operation_update=success_operation
            )

            logger.debug(
                "✅ Successfully completed child context for id: %s, name: %s",
                self.operation_identifier.operation_id,
                self.operation_identifier.name,
            )
            return raw_result  # noqa: TRY300
        except SuspendExecution:
            # Don't checkpoint SuspendExecution - let it bubble up
            raise
        except Exception as e:
            error_object = ErrorObject.from_exception(e)
            # Virtual deliberately does not write checkpoints, but exception still propagates below
            if not self.is_virtual:
                fail_operation: OperationUpdate = OperationUpdate.create_context_fail(
                    identifier=self.operation_identifier,
                    error=error_object,
                    sub_type=self.sub_type,
                )
                # Checkpoint child context FAIL with blocking (is_sync=True, default).
                # Must ensure the failure state is persisted before raising the exception.
                # This guarantees the error is durable and child operations won't be re-executed on replay.
                await self.state._create_checkpoint_async(
                    operation_update=fail_operation
                )

            # InvocationError and its derivatives can be retried.
            # When we encounter an invocation error (in all of its forms), we
            # bubble that error upwards (with the checkpoint in place for
            # non-virtual) such that we reach the execution handler at the
            # very top, which will then make the backend retry.
            if isinstance(e, InvocationError):
                raise
            raise error_object.to_callable_runtime_error() from e


async def child_handler(
    func: Callable[[], Awaitable[T]],
    state: ExecutionState,
    operation_identifier: OperationIdentifier,
    config: ChildConfig | None,
) -> T:
    """Run a function in a child context.

    Create a ChildOperationExecutor and delegates to its process() method.

    Args:
        func: The child context function to execute.
        state: The execution state.
        operation_identifier: The operation identifier for this child context.
        config: The child configuration (optional). When `config.is_virtual`
            is True, the child context does not checkpoint (START, SUCCEED, FAIL)
                        for itself.

    Returns:
        The result of executing the child context.

    Raises:
        May raise operation-specific errors during execution.
    """
    executor = ChildOperationExecutor(
        func,
        state,
        operation_identifier,
        config or ChildConfig(),
    )
    return await executor.process()


def durable_child_context(
    func: Callable[Params, Awaitable[T]],
) -> Callable[Params, Callable[[], Awaitable[T]]]:
    """Wrap an async function so calling it returns a zero-argument child context callable.

    The returned callable is suitable for passing to `run_in_child_context()`.
    """
    assert_async_callable(func)

    @functools.wraps(func)
    def wrapper(
        *args: Params.args, **kwargs: Params.kwargs
    ) -> Callable[[], Awaitable[T]]:
        return functools.partial(func, *args, **kwargs)

    return wrapper


@dataclass(frozen=True)
class DurableContext(OperationContext):
    lambda_context: LambdaContext | None = None
    step_id_prefix: str | None = None

    @functools.cached_property
    def step_counter(self):
        return OperationIdGenerator(self.operation_id_generator_prefix)

    @property
    def is_virtual(self) -> bool:
        return self.operation_identifier.parent_id != self.operation_id_generator_prefix

    @property
    def operation_id_generator_prefix(self):
        return (
            self.step_id_prefix
            if self.step_id_prefix is not None
            else self.operation_identifier.parent_id
        )

    def create_child_context(
        self, operation_id: str, *, is_virtual: bool = False
    ) -> DurableContext:
        """Create a child context for the given operation."""
        child_parent_id: str | None = self.parent_id if is_virtual else operation_id
        logger.debug(
            "Creating child context for operation %s (is_virtual=%s)",
            operation_id,
            is_virtual,
        )
        return DurableContext(
            execution_state=self.execution_state,
            operation_identifier=OperationIdentifier(
                operation_id=None,
                sub_type=OperationSubType.EXECUTION,
                parent_id=child_parent_id,
            ),
            lambda_context=self.lambda_context,
            step_id_prefix=operation_id,
        )


async def _run_in_child_context_in_context(
    context: DurableContext,
    func: Callable[[], Awaitable[T]],
    name: str | None = None,
    config: ChildConfig | None = None,
) -> T:
    assert_async_callable(func)
    step_name: str | None = name or get_callable_name(func)
    operation_id = context.step_counter.create_step_id()
    sub_type = (
        config.sub_type
        if config and config.sub_type
        else OperationSubType.RUN_IN_CHILD_CONTEXT
    )

    is_virtual: bool = config.is_virtual if config else False
    child_context = context.create_child_context(
        operation_id=operation_id,
        is_virtual=is_virtual,
    )

    async def callable_with_child_context():
        return await invoke_user_callable(
            child_context,
            func,
        )

    result = await child_handler(
        func=callable_with_child_context,
        state=context.execution_state,
        operation_identifier=OperationIdentifier(
            operation_id=operation_id,
            sub_type=sub_type,
            parent_id=context.parent_id,
            name=step_name,
        ),
        config=config,
    )
    context.execution_state.track_replay(operation_id=operation_id)
    return result


async def run_in_child_context(
    func: Callable[[], Awaitable[T]],
    name: str | None = None,
    config: ChildConfig | None = None,
) -> T:
    context = _get_durable_context("run_in_child_context")
    return await _run_in_child_context_in_context(
        context,
        func=func,
        name=name,
        config=config,
    )


def _get_durable_context(operation_name: str | None = None) -> DurableContext:
    current_context = get_current_context()
    if (
        current_context is None
        or not hasattr(current_context, "execution_state")
        or not hasattr(current_context, "operation_identifier")
        or not hasattr(current_context, "step_counter")
        or not hasattr(current_context, "create_child_context")
    ):
        msg = (
            f"{operation_name or 'Durable operations'} can only be used while a durable function or child "
            "context is executing."
        )
        raise RuntimeError(msg)
    return cast(DurableContext, current_context)

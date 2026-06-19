"""Implementation for the Durable create_callback and wait_for_callback operations."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Concatenate, Generic, TypeVar, ParamSpec

from ..async_tools import get_callable_name
from ..models import CallbackTimeoutType

from .child import _run_in_child_context_in_context, _get_durable_context
from ..async_tools import assert_async_callable
from ..config import StepConfig
from ..config import CallbackConfig, WaitForCallbackConfig
from ..context import (
    reset_current_context,
    set_current_context,
    get_current_context,
)
from ..exceptions import CallbackError, SuspendExecution
from ..models import (
    CallbackOptions,
    Operation,
    OperationIdentifier,
    OperationUpdate,
    OperationSubType,
)
from .base import (
    CheckpointedResult,
    OperationExecutor,
    OperationContext,
    get_checkpoint_result,
)
from ..serdes import deserialize, SerDes, PassThroughSerDes

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ..state import ExecutionState
    from .child import DurableContext

T = TypeVar("T")  # Result type
Params = ParamSpec("Params")

logger = logging.getLogger(__name__)

PASS_THROUGH_SERDES: SerDes[Any] = PassThroughSerDes()


class CallbackOperationExecutor(OperationExecutor[str]):
    """Executor for callback operations."""

    def __init__(
        self,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        config: CallbackConfig | None,
    ):
        """Initialize the callback operation executor.

        Args:
            state: The execution state
            operation_identifier: The operation identifier
            config: The callback configuration (optional)
        """
        super().__init__(state=state, operation_identifier=operation_identifier)
        self.config = config

    async def start(self) -> str:
        """Start a new callback operation."""
        callback_options: CallbackOptions = (
            CallbackOptions(
                timeout_seconds=self.config.timeout_seconds,
                heartbeat_timeout_seconds=self.config.heartbeat_timeout_seconds,
            )
            if self.config
            else CallbackOptions()
        )

        create_callback_operation: OperationUpdate = OperationUpdate.create_callback(
            identifier=self.operation_identifier,
            callback_options=callback_options,
        )

        await self.create_checkpoint(create_callback_operation)

        checkpointed_result = self.get_checkpointed_result()
        if not checkpointed_result.operation:
            msg = f"Missing callback details for operation: {self.operation_identifier.operation_id}"
            raise CallbackError(msg)
        return await self.replay(checkpointed_result.operation)

    async def replay(self, operation: Operation) -> str:
        """Replay an existing callback operation from its checkpoint."""
        if not operation.callback_details:
            msg = (
                f"Missing callback details for operation: "
                f"{self.operation_identifier.operation_id}"
            )
            raise CallbackError(msg)

        return await self.execute(CheckpointedResult.create_from_operation(operation))

    async def execute(self, checkpointed_result: CheckpointedResult) -> str:
        """Execute callback operation by extracting the callback_id.

        Callbacks don't execute logic - they just extract and return the callback_id
        from the checkpoint data.

        Args:
            checkpointed_result: The checkpoint data containing callback_details

        Returns:
            The callback_id from the checkpoint

        Raises:
            CallbackError: If callback_details are missing (should never happen)
        """
        if (
            not checkpointed_result.operation
            or not checkpointed_result.operation.callback_details
        ):
            msg = f"Missing callback details for operation: {self.operation_identifier.operation_id}"
            raise CallbackError(msg)

        return checkpointed_result.operation.callback_details.callback_id


async def wait_for_callback_handler(
    context: DurableContext,
    submitter: Callable[[str], Awaitable[Any]],
    name: str | None = None,
    config: WaitForCallbackConfig | None = None,
) -> Any:
    """Wait for a callback to be invoked by an external system.

    This is a helper function that is used to create a callback and wait for it to be invoked by an external system.
    """
    from .step import step as step_operation

    name_with_space: str = f"{name} " if name else ""
    callback: Callback = await create_callback(
        name=f"{name_with_space}create callback id", config=config
    )

    async def submitter_step():
        step_context = get_current_context()
        callback_context = WaitForCallbackContext(
            callback_id=callback.callback_id,
            execution_state=step_context.execution_state,
            operation_identifier=step_context.operation_identifier,
        )
        token = set_current_context(callback_context)
        try:
            return await submitter(callback.callback_id)
        finally:
            reset_current_context(token)

    step_config = (
        StepConfig(
            retry_strategy=config.retry_strategy,
            serdes=config.serdes,
        )
        if config
        else None
    )
    await step_operation(
        func=submitter_step,
        name=f"{name_with_space}submitter",
        config=step_config,
    )

    return await callback.result()


async def create_callback(
    name: str | None = None, config: CallbackConfig | None = None
) -> Callback:
    """Create a durable callback handle that external systems can complete later."""
    context = _get_durable_context("create_callback")
    if not config:
        config = CallbackConfig()
    operation_id: str = context.step_counter.create_step_id()

    executor: CallbackOperationExecutor = CallbackOperationExecutor(
        state=context.execution_state,
        operation_identifier=OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.CALLBACK,
            parent_id=context.parent_id,
            name=name,
        ),
        config=config,
    )
    callback_id: str = await executor.process()
    result: Callback = Callback(
        callback_id=callback_id,
        operation_id=operation_id,
        state=context.execution_state,
        serdes=config.serdes,
    )
    context.execution_state.track_replay(operation_id=operation_id)
    return result


def durable_wait_for_callback(
    func: Callable[Concatenate[str, Params], Awaitable[T]],
) -> Callable[Params, Callable[[str], Awaitable[T]]]:
    """Wrap your callable into a wait_for_callback submitter function.

    This decorator allows you to define a submitter function with additional
    parameters that will be bound when called.

    Args:
        func: A callable that takes callback_id and additional parameters

    Returns:
        A wrapper function that binds the additional parameters and returns
        a submitter function compatible with wait_for_callback

    Example:
        @durable_wait_for_callback
        async def submit_to_external_system(
            callback_id: str,
            task_name: str,
            priority: int
        ):
            logging.getLogger(__name__).info(
                "Submitting %s with callback %s", task_name, callback_id
            )
            external_api.submit_task(
                task_name=task_name,
                priority=priority,
                callback_id=callback_id
            )

        # Usage in durable handler:
        result = await wait_for_callback(
            submit_to_external_system("my_task", priority=5)
        )
    """
    assert_async_callable(func)

    def wrapper(*args, **kwargs):
        async def submitter_with_arguments(callback_id: str):
            return await func(callback_id, *args, **kwargs)

        submitter_with_arguments._original_name = func.__name__  # noqa: SLF001
        return submitter_with_arguments

    return wrapper


class Callback(Generic[T]):  # noqa: PYI059
    """A future that will block on result() until callback_id returns."""

    def __init__(
        self,
        callback_id: str,
        operation_id: str,
        state: ExecutionState,
        serdes: SerDes[T] | None = None,
    ):
        self.callback_id: str = callback_id
        self.operation_id: str = operation_id
        self.state: ExecutionState = state
        self.serdes: SerDes[T] | None = serdes

    async def result(self) -> T | None:
        """Return the result of the future. Will block until result is available.

        This will suspend the current execution while waiting for the result to
        become available. Durable Functions will replay the execution once the
        result is ready, and proceed when it reaches the .result() call.

        Use the callback id with the following APIs to send back the result, error or
        heartbeats: SendDurableExecutionCallbackSuccess, SendDurableExecutionCallbackFailure
        and SendDurableExecutionCallbackHeartbeat.
        """
        checkpointed_result: CheckpointedResult = get_checkpoint_result(
            self.state,
            self.operation_id,
        )

        if not checkpointed_result.is_existent():
            msg = "Callback operation must exist"
            raise CallbackError(message=msg, callback_id=self.callback_id)

        if (
            checkpointed_result.is_failed()
            or checkpointed_result.is_cancelled()
            or checkpointed_result.is_timed_out()
            or checkpointed_result.is_stopped()
        ):
            msg = _format_callback_error_message(checkpointed_result)
            raise CallbackError(message=msg, callback_id=self.callback_id)

        if checkpointed_result.is_succeeded():
            if checkpointed_result.result is None:
                return None  # type: ignore

            return deserialize(
                serdes=self.serdes if self.serdes is not None else PASS_THROUGH_SERDES,
                data=checkpointed_result.result,
                operation_id=self.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )

        # operation exists; it has not terminated (successfully or otherwise)
        # therefore we should wait
        msg = "Callback result not received yet. Suspending execution while waiting for result."
        raise SuspendExecution(msg)


async def wait_for_callback(
    submitter: Callable[[str], Awaitable[Any]],
    name: str | None = None,
    config: WaitForCallbackConfig | None = None,
) -> Any:
    """Create a callback, run a submitter, then suspend until the callback resolves."""
    context = _get_durable_context("wait_for_callback")
    assert_async_callable(submitter, label="submitter")
    step_name: str | None = name or get_callable_name(submitter)
    logger.debug("wait_for_callback name: %s", step_name)

    async def wait_in_child_context():
        current_context = get_current_context()
        return await wait_for_callback_handler(
            current_context,
            submitter,
            step_name,
            config,
        )

    return await _run_in_child_context_in_context(
        context,
        wait_in_child_context,
        name=step_name,
    )


@dataclass(frozen=True)
class WaitForCallbackContext(OperationContext):
    """Context available during wait_for_callback submitter execution."""

    callback_id: str = ""


def _format_callback_error_message(checkpointed_result: CheckpointedResult) -> str:
    """Build a stable callback error message from checkpoint state."""
    error = checkpointed_result.error
    if not error or not error.message:
        return "Callback failed"

    message = error.message
    if (
        checkpointed_result.is_timed_out()
        and error.type in {timeout.value for timeout in CallbackTimeoutType}
        and error.type not in message
    ):
        return f"{message}: {error.type}"

    return message

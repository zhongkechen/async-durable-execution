from __future__ import annotations

import hashlib
import logging
import functools
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    ParamSpec,
    TypeVar,
    cast,
    overload,
)

from async_durable_execution.async_tools import (
    assert_async_callable,
    get_callable_name,
    invoke_callable_with_optional_context,
)
from async_durable_execution.config import (
    BatchedInput,
    CallbackConfig,
    ChildConfig,
    InvokeConfig,
    MapConfig,
    ParallelBranch,
    ParallelConfig,
    StepConfig,
    WaitForCallbackConfig,
    duration_to_seconds,
)
from async_durable_execution.exceptions import (
    CallbackError,
    SuspendExecution,
    ValidationError,
)
from async_durable_execution.models import OperationIdentifier
from async_durable_execution.models import (
    CallbackTimeoutType,
    OperationSubType,
)
from async_durable_execution.retries import WithRetryConfig, create_retry_strategy
from async_durable_execution.logger import Logger, LogInfo
from async_durable_execution.operation.child import child_handler
from async_durable_execution.operation.map import map_handler
from async_durable_execution.operation.parallel import parallel_handler
from async_durable_execution.serdes import (
    PassThroughSerDes,
    SerDes,
    deserialize,
)
from async_durable_execution.state import ExecutionState  # noqa: TC001
from async_durable_execution.types import Callback as CallbackProtocol
from async_durable_execution.types import (
    Context,
    DurableContext as DurableContextProtocol,
)
from async_durable_execution.types import (
    LoggerInterface,
    StepContext,
    WaitForCallbackContext,
    WaitForConditionCheckContext,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from async_durable_execution.concurrency.models import BatchResult
    from async_durable_execution.state import CheckpointedResult
    from async_durable_execution.types import LambdaContext
    from async_durable_execution.waits import WaitForConditionConfig

P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type
T = TypeVar("T")
U = TypeVar("U")
Params = ParamSpec("Params")


logger = logging.getLogger(__name__)

PASS_THROUGH_SERDES: SerDes[Any] = PassThroughSerDes()

_current_context: ContextVar[Context | None] = ContextVar(
    "async_durable_execution.current_context",
    default=None,
)


def get_context() -> Context:
    """Return the current durable or step context."""
    current_context = _current_context.get()
    if current_context is None:
        msg = (
            "get_context() can only be used while a durable function or step "
            "function is executing."
        )
        raise RuntimeError(msg)
    return current_context


def get_step_context() -> StepContext:
    """Return the StepContext for the currently executing step."""
    current_context = _current_context.get()
    if current_context is None or not isinstance(current_context, StepContext):
        msg = "get_step_context() can only be used while a step function is executing."
        raise RuntimeError(msg)
    return current_context


def get_attempt() -> int | None:
    return get_step_context().attempt


def get_logger() -> LoggerInterface:
    """Return the replay-aware logger for the current durable or step context."""
    return get_context().logger


def _set_context(context: Context) -> Token[Context | None]:
    return _current_context.set(context)


def _reset_context(token: Token[Context | None]) -> None:
    _current_context.reset(token)


def _set_step_context(step_context: StepContext) -> Token[Context | None]:
    return _set_context(step_context)


def _reset_step_context(token: Token[Context | None]) -> None:
    _reset_context(token)


def _get_durable_context(operation_name: str) -> DurableContext:
    current_context = get_context()
    if isinstance(current_context, StepContext):
        msg = (
            f"{operation_name}() can only be used while a durable function or child "
            "context is executing."
        )
        raise RuntimeError(msg)
    return cast(DurableContext, current_context)


from async_durable_execution.operation.callback import (  # noqa: E402
    CallbackOperationExecutor,
    wait_for_callback_handler,
)
from async_durable_execution.operation.invoke import InvokeOperationExecutor  # noqa: E402
from async_durable_execution.operation.step import StepOperationExecutor  # noqa: E402
from async_durable_execution.operation.wait import WaitOperationExecutor  # noqa: E402
from async_durable_execution.operation.wait_for_condition import (  # noqa: E402
    WaitForConditionOperationExecutor,
)


class _StepCounter:
    def __init__(self) -> None:
        self._value = 0

    def increment(self) -> int:
        self._value += 1
        return self._value

    def get_current(self) -> int:
        return self._value


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


@dataclass(frozen=True)
class ExecutionContext:
    """Readonly metadata about the current durable execution context.

    This class provides immutable access to execution-level metadata.

    Attributes:
        durable_execution_arn: The Amazon Resource Name (ARN) of the current
            durable execution.
    """

    durable_execution_arn: str


async def create_callback(
    name: str | None = None, config: CallbackConfig | None = None
) -> Callback:
    return await _get_durable_context("create_callback").create_callback(
        name=name,
        config=config,
    )


async def invoke(
    function_name: str,
    payload: P,
    name: str | None = None,
    config: InvokeConfig[P, R] | None = None,
) -> R:
    return await _get_durable_context("invoke").invoke(
        function_name=function_name,
        payload=payload,
        name=name,
        config=config,
    )


async def map(
    inputs: Sequence[U],
    func: Callable[[U | BatchedInput[Any, U], int, Sequence[U]], Awaitable[T]],
    name: str | None = None,
    config: MapConfig | None = None,
):
    return await _get_durable_context("map").map(
        inputs=inputs,
        func=func,
        name=name,
        config=config,
    )


async def parallel(
    functions: Sequence[Callable[[], Awaitable[T]] | ParallelBranch[T]],
    name: str | None = None,
    config: ParallelConfig | None = None,
):
    return await _get_durable_context("parallel").parallel(
        functions=functions,
        name=name,
        config=config,
    )


async def run_in_child_context(
    func: Callable[[], Awaitable[T]],
    name: str | None = None,
    config: ChildConfig | None = None,
) -> T:
    return await _get_durable_context("run_in_child_context").run_in_child_context(
        func=func,
        name=name,
        config=config,
    )


async def step(
    func: Callable[[], Awaitable[T]],
    name: str | None = None,
    config: StepConfig | None = None,
) -> T:
    return await _get_durable_context("step").step(
        func=func,
        name=name,
        config=config,
    )


async def wait(duration: timedelta, name: str | None = None) -> None:
    await _get_durable_context("wait").wait(duration=duration, name=name)


async def wait_for_callback(
    submitter: Callable[[str, WaitForCallbackContext], Awaitable[Any]],
    name: str | None = None,
    config: WaitForCallbackConfig | None = None,
) -> Any:
    return await _get_durable_context("wait_for_callback").wait_for_callback(
        submitter=submitter,
        name=name,
        config=config,
    )


async def wait_for_condition(
    check: Callable[[T, WaitForConditionCheckContext], Awaitable[T]],
    config: WaitForConditionConfig[T],
    name: str | None = None,
) -> T:
    return await _get_durable_context("wait_for_condition").wait_for_condition(
        check=check,
        config=config,
        name=name,
    )


def set_logger(new_logger: LoggerInterface) -> None:
    _get_durable_context("set_logger").set_logger(new_logger)


@overload
async def with_retry(
    context: DurableContext,
    func: Callable[[int], Awaitable[T]],
    config: WithRetryConfig[T],
    name: str | None = None,
) -> T: ...


@overload
async def with_retry(
    context: Callable[[int], Awaitable[T]],
    func: WithRetryConfig[T],
    config: None = None,
    name: str | None = None,
) -> T: ...


async def with_retry(
    context: DurableContext | Callable[[int], Awaitable[T]],
    func: Callable[[int], Awaitable[T]] | WithRetryConfig[T],
    config: WithRetryConfig[T] | None = None,
    name: str | None = None,
) -> T:
    """Retry a block of durable logic with configurable backoff."""

    resolved_context: DurableContext
    resolved_func: Callable[[int], Awaitable[T]]
    resolved_config: WithRetryConfig[T]

    if config is None:
        if not callable(context) or not isinstance(func, WithRetryConfig):
            msg = (
                "with_retry() expects either "
                "(context, func, config, name=...) or (func, config, name=...)."
            )
            raise TypeError(msg)
        current_context = get_context()
        if isinstance(current_context, StepContext):
            msg = "with_retry() must run inside a durable context."
            raise RuntimeError(msg)
        resolved_context = cast(DurableContext, current_context)
        resolved_func = cast("Callable[[int], Awaitable[T]]", context)
        resolved_config = func
    else:
        resolved_context = cast(DurableContext, context)
        resolved_func = cast("Callable[[int], Awaitable[T]]", func)
        resolved_config = config

    async def run_loop(*_ignored_args) -> T:
        assert_async_callable(resolved_func)
        try:
            ctx = get_context()
        except RuntimeError:
            if not _ignored_args:
                raise
            ctx = _ignored_args[0]
        if isinstance(ctx, StepContext):
            msg = "with_retry() must run inside a durable context."
            raise RuntimeError(msg)
        durable_ctx = cast(DurableContext, ctx)
        retry_strategy = resolved_config.retry_strategy or create_retry_strategy()
        attempt = 0
        while True:
            attempt += 1
            try:
                invoke_with_context = getattr(
                    durable_ctx, "_invoke_user_callable", None
                )
                if invoke_with_context is not None:
                    return await invoke_with_context(
                        resolved_func,
                        attempt,
                        context_position="prepend",
                    )
                return await invoke_callable_with_optional_context(
                    resolved_func,
                    durable_ctx,
                    attempt,
                    context_position="prepend",
                )
            except SuspendExecution:
                raise
            except Exception as err:
                decision = retry_strategy(err, attempt)
                if not decision.should_retry:
                    raise
                wait_name = f"{name}-backoff-{attempt}" if name else None
                await durable_ctx.wait(duration=decision.delay, name=wait_name)

    if resolved_config.wrap_with_run_in_child_context:
        return await resolved_context.run_in_child_context(
            run_loop,
            name=name,
            config=resolved_config.child_context_config,
        )

    invoke_with_context = getattr(resolved_context, "_invoke_user_callable", None)
    if invoke_with_context is not None:
        return await invoke_with_context(run_loop)

    token = _set_context(resolved_context)
    try:
        return await run_loop()
    finally:
        _reset_context(token)


def durable_step(
    func: Callable[Params, Awaitable[T]],
) -> Callable[Params, Callable[[], Awaitable[T]]]:
    """Wrap an async function so calling it returns a zero-argument step callable.

    The returned callable is suitable for passing to `step()` or `context.step()`,
    which keeps durable step creation explicit while avoiding manual `partial(...)`
    wrapping at the callsite.
    """
    assert_async_callable(func)

    @functools.wraps(func)
    def wrapper(
        *args: Params.args, **kwargs: Params.kwargs
    ) -> Callable[[], Awaitable[T]]:
        return functools.partial(func, *args, **kwargs)

    return wrapper


def durable_parallel_branch(
    name: str | None = None,
) -> Callable[
    [Callable[Params, Awaitable[T]]],
    Callable[Params, ParallelBranch[T]],
]:
    """Wrap your callable into a named ParallelBranch for use with context.parallel().

    This is a decorator factory — call it with an optional name to produce
    the actual decorator.

    Args:
        name: Optional custom name for this branch. When provided, replaces
            the default "parallel-branch-{index}" naming in execution history.
            If None, the function's __name__ is used.

    Example:
        @durable_parallel_branch(name="fetch-user-data")
        async def fetch_user(user_id: str) -> dict:
            ctx = get_context()

            async def load_user() -> dict:
                return {"id": user_id, "name": "Jane"}

            return await ctx.step(load_user, name="load_user")

        @durable_parallel_branch(name="fetch-orders")
        async def fetch_orders(user_id: str) -> list:
            ctx = get_context()

            async def load_orders() -> list:
                return ["order1", "order2"]

            return await ctx.step(load_orders, name="load_orders")

        # Usage in a durable handler:
        results = await context.parallel(
            functions=[fetch_user(user_id), fetch_orders(user_id)],
            name="load-data",
        )
    """

    def decorator(
        func: Callable[Params, Awaitable[T]],
    ) -> Callable[Params, ParallelBranch[T]]:
        assert_async_callable(func)

        def wrapper(*args, **kwargs) -> ParallelBranch[T]:
            async def function_with_arguments(*runtime_args, **runtime_kwargs) -> T:
                current_context = runtime_args[0] if runtime_args else get_context()
                if runtime_kwargs:
                    msg = "Parallel branches do not accept runtime keyword arguments."
                    raise TypeError(msg)
                return await invoke_callable_with_optional_context(
                    func,
                    current_context,
                    *args,
                    context_position="prepend",
                    **kwargs,
                )

            return ParallelBranch(func=function_with_arguments, name=name)

        return wrapper

    return decorator


def durable_wait_for_callback(
    func: Callable[Concatenate[str, WaitForCallbackContext, Params], Awaitable[T]],
) -> Callable[Params, Callable[[str, WaitForCallbackContext], Awaitable[T]]]:
    """Wrap your callable into a wait_for_callback submitter function.

    This decorator allows you to define a submitter function with additional
    parameters that will be bound when called.

    Args:
        func: A callable that takes callback_id, context, and additional parameters

    Returns:
        A wrapper function that binds the additional parameters and returns
        a submitter function compatible with wait_for_callback

    Example:
        @durable_wait_for_callback
        async def submit_to_external_system(
            callback_id: str,
            context: WaitForCallbackContext,
            task_name: str,
            priority: int
        ):
            context.logger.info(f"Submitting {task_name} with callback {callback_id}")
            external_api.submit_task(
                task_name=task_name,
                priority=priority,
                callback_id=callback_id
            )

        # Usage in durable handler:
        result = await context.wait_for_callback(
            submit_to_external_system("my_task", priority=5)
        )
    """
    assert_async_callable(func)

    def wrapper(*args, **kwargs):
        async def submitter_with_arguments(
            callback_id: str, context: WaitForCallbackContext
        ):
            return await func(callback_id, context, *args, **kwargs)

        submitter_with_arguments._original_name = func.__name__  # noqa: SLF001
        return submitter_with_arguments

    return wrapper


class Callback(Generic[T], CallbackProtocol[T]):  # noqa: PYI059
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
        checkpointed_result: CheckpointedResult = self.state.get_checkpoint_result(
            self.operation_id
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


class DurableContext(DurableContextProtocol):
    def __init__(
        self,
        state: ExecutionState,
        execution_context: ExecutionContext,
        lambda_context: LambdaContext | None = None,
        parent_id: str | None = None,
        logger: Logger | None = None,
        step_id_prefix: str | None = None,
    ) -> None:
        self.state: ExecutionState = state
        self.execution_context: ExecutionContext = execution_context
        self.lambda_context = lambda_context
        # operations inside this context use this id as their parent
        self._parent_id: str | None = parent_id
        # child operations use this to generate deterministic step ids.
        # differs from `parent_id` only for virtual contexts.
        self._step_id_prefix: str | None = (
            step_id_prefix if step_id_prefix is not None else parent_id
        )
        # cached at construction to make invariant even if parent/prefix mutates.
        self._is_virtual: bool = self._parent_id != self._step_id_prefix
        self._step_counter = _StepCounter()

        log_info = LogInfo(
            execution_state=state,
            parent_id=parent_id,
        )
        self._log_info = log_info
        self.logger: Logger = logger or Logger.from_log_info(
            logger=logging.getLogger(),
            info=log_info,
        )

    @property
    def is_virtual(self) -> bool:
        """True if this context does not checkpoint its own start and completion.

        You create a virtual context by `create_child_context(..., is_virtual=True)`.
        FLAT-mode `map`/`parallel` branches uses virtual contexts. Inner operations
        use the grandfather as parent (enclosing non-virtual ancestor, skipping the branch level
        in the hierarchy), while step ids are still prefixed with the branch's own
        operation id so replay stays deterministic.
        """
        return self._is_virtual

    @staticmethod
    def from_lambda_context(
        state: ExecutionState,
        lambda_context: LambdaContext,
    ):
        return DurableContext(
            state=state,
            execution_context=ExecutionContext(
                durable_execution_arn=state.durable_execution_arn
            ),
            lambda_context=lambda_context,
            parent_id=None,
        )

    def create_child_context(
        self, operation_id: str, *, is_virtual: bool = False
    ) -> DurableContext:
        """Create a child context for the given operation.

        Args:
            operation_id: The operation id that owns the child context. Used as
                the child's step-id prefix in all cases.
            is_virtual: When `True`, create a virtual child whose inner
                operations report to this context's own `_parent_id` (one
                level up the hierarchy). When `False` (default), produce a
                regular child whose inner operations report to
                `operation_id`.

        Returns:
            A new `DurableContext` child for the current context.
        """
        # For a virtual child, propagate the current `_parent_id` so its
        # inner operations refer to the grandparent rather than the parent.
        # For a regular non-virtual child, the child's own `operation_id` is
        # the parent id for its inner operations (standard nesting).
        child_parent_id: str | None = self._parent_id if is_virtual else operation_id
        logger.debug(
            "Creating child context for operation %s (is_virtual=%s)",
            operation_id,
            is_virtual,
        )
        return DurableContext(
            state=self.state,
            execution_context=self.execution_context,
            lambda_context=self.lambda_context,
            parent_id=child_parent_id,
            step_id_prefix=operation_id,
            logger=self.logger.with_log_info(
                LogInfo(
                    execution_state=self.state,
                    parent_id=child_parent_id,
                )
            ),
        )

    @staticmethod
    def _resolve_step_name(name: str | None, func: Callable) -> str | None:
        """Resolve the step name.

        Returns:
            str | None: The provided name, and if that doesn't exist the callable function's name if it has one.
        """
        return name or get_callable_name(func)

    def set_logger(self, new_logger: LoggerInterface):
        """Set the logger for the current context."""
        self.logger = Logger.from_log_info(
            logger=new_logger,
            info=self._log_info,
        )

    async def _invoke_user_callable(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        context_position: str = "prepend",
        **kwargs,
    ) -> T:
        token = _set_context(self)
        try:
            return await invoke_callable_with_optional_context(
                func,
                self,
                *args,
                context_position=context_position,
                **kwargs,
            )
        finally:
            _reset_context(token)

    def _create_step_id_for_logical_step(self, step: int) -> str:
        """
        Generate a step_id based on the given logical step.
        This allows us to recover operation ids or even look
        forward without changing the internal state of this context.
        """
        prefix: str | None = self._step_id_prefix
        step_id: str = f"{prefix}-{step}" if prefix else str(step)
        return hashlib.blake2b(step_id.encode()).hexdigest()[:64]

    def _create_step_id(self) -> str:
        """Generate a step id, incrementing in order of invocation.

        This method is an internal implementation detail. Do not rely the exact format of
        the id generated by this method. It is subject to change without notice.
        """
        new_counter: int = self._step_counter.increment()
        return self._create_step_id_for_logical_step(new_counter)

    async def create_callback(
        self, name: str | None = None, config: CallbackConfig | None = None
    ) -> Callback:
        """Create a callback.

        This generates a future with a callback id. External systems can signal
        your Durable Function to proceed by using this callback id with the
        SendDurableExecutionCallbackSuccess, SendDurableExecutionCallbackFailure and
        SendDurableExecutionCallbackHeartbeat APIs.

        Args:
            name (str): Optional name for the operation.
            config (CallbackConfig): Configuration for the callback.

        Return:
            Callback future. Use result() on this future to wait for the callback resuilt.
        """
        if not config:
            config = CallbackConfig()
        operation_id: str = self._create_step_id()

        executor: CallbackOperationExecutor = CallbackOperationExecutor(
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                sub_type=OperationSubType.CALLBACK,
                parent_id=self._parent_id,
                name=name,
            ),
            config=config,
        )
        callback_id: str = await executor.process()
        result: Callback = Callback(
            callback_id=callback_id,
            operation_id=operation_id,
            state=self.state,
            serdes=config.serdes,
        )
        self.state.track_replay(operation_id=operation_id)
        return result

    async def invoke(
        self,
        function_name: str,
        payload: P,
        name: str | None = None,
        config: InvokeConfig[P, R] | None = None,
    ) -> R:
        """Invoke another Durable Function.

        Args:
            function_name: Name of the function to invoke
            payload: Input payload to send to the function
            name: Optional name for the operation
            config: Optional configuration for the invoke operation

        Returns:
            The result of the invoked function
        """
        if not config:
            config = InvokeConfig[P, R]()
        operation_id = self._create_step_id()

        executor: InvokeOperationExecutor[R] = InvokeOperationExecutor(
            function_name=function_name,
            payload=payload,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                sub_type=OperationSubType.CHAINED_INVOKE,
                parent_id=self._parent_id,
                name=name,
            ),
            config=config,
        )
        result: R = await executor.process()
        self.state.track_replay(operation_id=operation_id)
        return result

    async def map(
        self,
        inputs: Sequence[U],
        func: Callable[[U | BatchedInput[Any, U], int, Sequence[U]], Awaitable[T]],
        name: str | None = None,
        config: MapConfig | None = None,
    ):
        """Execute a callable for each item in parallel."""
        assert_async_callable(func)
        map_name: str | None = self._resolve_step_name(name, func)

        operation_id = self._create_step_id()
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.MAP,
            parent_id=self._parent_id,
            name=map_name,
        )
        map_context = self.create_child_context(operation_id=operation_id)

        async def map_in_child_context() -> BatchResult[T]:
            # map_context is a child_context of the context upon which `.map`
            # was called. We are calling it `map_context` to make it explicit
            # that any operations happening from hereon are done on the context
            # that owns the branches
            return await map_handler(
                items=inputs,
                func=func,
                config=config,
                execution_state=self.state,
                map_context=map_context,
                operation_identifier=operation_identifier,
            )

        result = await child_handler(
            func=map_in_child_context,
            state=self.state,
            operation_identifier=operation_identifier,
            config=ChildConfig(
                sub_type=OperationSubType.MAP,
                serdes=getattr(config, "serdes", None),
                # child_handler should only know the serdes of the parent serdes,
                # the item serdes will be passed when we are actually executing
                # the branch within its own child_handler.
                item_serdes=None,
            ),
        )
        self.state.track_replay(operation_id=operation_id)
        return result

    async def parallel(
        self,
        functions: Sequence[Callable[[], Awaitable[T]] | ParallelBranch[T]],
        name: str | None = None,
        config: ParallelConfig | None = None,
    ):
        """Execute multiple callables in parallel."""
        for index, function in enumerate(functions):
            target = function.func if isinstance(function, ParallelBranch) else function
            assert_async_callable(target, label=f"functions[{index}]")

        # _create_step_id() is thread-safe. rest of method is safe, since using local copy of parent id
        operation_id = self._create_step_id()
        parallel_context = self.create_child_context(operation_id=operation_id)
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.PARALLEL,
            parent_id=self._parent_id,
            name=name,
        )

        async def parallel_in_child_context() -> BatchResult[T]:
            # parallel_context is a child_context of the context upon which `.map`
            # was called. We are calling it `parallel_context` to make it explicit
            # that any operations happening from hereon are done on the context
            # that owns the branches
            return await parallel_handler(
                callables=functions,
                config=config,
                execution_state=self.state,
                parallel_context=parallel_context,
                operation_identifier=operation_identifier,
            )

        result = await child_handler(
            func=parallel_in_child_context,
            state=self.state,
            operation_identifier=operation_identifier,
            config=ChildConfig(
                sub_type=OperationSubType.PARALLEL,
                serdes=getattr(config, "serdes", None),
                # child_handler should only know the serdes of the parent serdes,
                # the item serdes will be passed when we are actually executing
                # the branch within its own child_handler.
                item_serdes=None,
            ),
        )
        self.state.track_replay(operation_id=operation_id)
        return result

    async def run_in_child_context(
        self,
        func: Callable[[], Awaitable[T]],
        name: str | None = None,
        config: ChildConfig | None = None,
    ) -> T:
        """Run the callable and pass a child context to it.

        Use this to nest and group operations.

        Args:
            callable (Callable[[], T]): Run this callable inside a child durable
                context. Access that context with get_context().
            name (str | None): name for the operation.
            config (ChildConfig | None = None): child context configuration.

        Returns:
            T: The result of the callable.
        """
        assert_async_callable(func)
        step_name: str | None = self._resolve_step_name(name, func)
        # _create_step_id() is thread-safe. rest of method is safe, since using local copy of parent id
        operation_id = self._create_step_id()
        sub_type = (
            config.sub_type
            if config and config.sub_type
            else OperationSubType.RUN_IN_CHILD_CONTEXT
        )

        is_virtual: bool = config.is_virtual if config else False

        child_context = self.create_child_context(
            operation_id=operation_id,
            is_virtual=is_virtual,
        )

        async def callable_with_child_context():
            return await child_context._invoke_user_callable(
                func,
                context_position="prepend",
            )

        result = await child_handler(
            func=callable_with_child_context,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                sub_type=sub_type,
                parent_id=self._parent_id,
                name=step_name,
            ),
            config=config,
        )
        self.state.track_replay(operation_id=operation_id)
        return result

    async def step(
        self,
        func: Callable[[], Awaitable[T]],
        name: str | None = None,
        config: StepConfig | None = None,
    ) -> T:
        assert_async_callable(func)
        step_name = name or get_callable_name(func, include_original_name=False)
        logger.debug("Step name: %s", step_name)
        if not config:
            config = StepConfig()
        operation_id = self._create_step_id()

        executor: StepOperationExecutor[T] = StepOperationExecutor(
            func=func,
            config=config,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                sub_type=OperationSubType.STEP,
                parent_id=self._parent_id,
                name=step_name,
            ),
            context_logger=self.logger,
        )
        result: T = await executor.process()
        self.state.track_replay(operation_id=operation_id)
        return result

    async def wait(self, duration: timedelta, name: str | None = None) -> None:
        """Wait for a specified amount of time.

        Args:
            duration: Length of time to wait
            name: Optional name for the wait step
        """
        seconds = duration_to_seconds(duration)
        if seconds < 1:
            msg = "duration must be at least 1 second"
            raise ValidationError(msg)
        operation_id = self._create_step_id()

        executor: WaitOperationExecutor = WaitOperationExecutor(
            seconds=seconds,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                sub_type=OperationSubType.WAIT,
                parent_id=self._parent_id,
                name=name,
            ),
        )
        await executor.process()
        self.state.track_replay(operation_id=operation_id)

    async def wait_for_callback(
        self,
        submitter: Callable[[str, WaitForCallbackContext], Awaitable[Any]],
        name: str | None = None,
        config: WaitForCallbackConfig | None = None,
    ) -> Any:
        assert_async_callable(submitter, label="submitter")
        step_name: str | None = self._resolve_step_name(name, submitter)
        logger.debug("wait_for_callback name: %s", step_name)

        async def wait_in_child_context():
            current_context = get_context()
            return await wait_for_callback_handler(
                current_context,
                submitter,
                step_name,
                config,
            )

        return await self.run_in_child_context(
            wait_in_child_context,
            step_name,
        )

    async def wait_for_condition(
        self,
        check: Callable[[T, WaitForConditionCheckContext], Awaitable[T]],
        config: WaitForConditionConfig[T],
        name: str | None = None,
    ) -> T:
        """Wait for a condition to be met by polling.

        Args:
            check (Callable[[T, WaitForConditionCheckContext], T]): Function that checks the condition and returns updated state
            config (WaitForConditionConfig[T]): Configuration including wait strategy and initial state
            name (str | None): Optional name for the operation

        Returns:
            The final state when condition is met.
        """
        if check is None:
            msg = "`check` is required for wait_for_condition"
            raise ValidationError(msg)
        if not config:
            msg = "`config` is required for wait_for_condition"
            raise ValidationError(msg)
        assert_async_callable(check, label="check")

        operation_id = self._create_step_id()

        executor: WaitForConditionOperationExecutor[T] = (
            WaitForConditionOperationExecutor(
                check=check,
                config=config,
                state=self.state,
                operation_identifier=OperationIdentifier(
                    operation_id=operation_id,
                    sub_type=OperationSubType.WAIT_FOR_CONDITION,
                    parent_id=self._parent_id,
                    name=name,
                ),
                context_logger=self.logger,
            )
        )
        result: T = await executor.process()
        self.state.track_replay(operation_id=operation_id)
        return result

"""Implementation for Durable Parallel operation."""

from __future__ import annotations
import json
import logging
from typing import TYPE_CHECKING, TypeVar, Sequence, Callable, Awaitable, ParamSpec

from .base import get_checkpoint_result
from .child import child_handler, _get_durable_context

from ..async_tools import (
    invoke_user_callable,
    invoke_callable,
    assert_async_callable,
)
from async_durable_execution.operation.concurrency import ConcurrentExecutor
from ..config import (
    NestingType,
    ParallelBranch,
    ParallelConfig,
    ChildConfig,
)
from ..models import Executable, OperationIdentifier, OperationSubType


if TYPE_CHECKING:
    from ..context import get_current_context
    from ..models import BatchResult
    from ..serdes import SerDes
    from ..state import ExecutionState
    from ..types import SummaryGenerator
    from .child import DurableContext

logger = logging.getLogger(__name__)

# Result type
R = TypeVar("R")
T = TypeVar("T")
Params = ParamSpec("Params")


class ParallelExecutor(ConcurrentExecutor[Callable, R]):
    def __init__(
        self,
        executables: list[Executable[Callable]],
        max_concurrency: int | None,
        completion_config,
        top_level_sub_type: OperationSubType,
        iteration_sub_type: OperationSubType,
        name_prefix: str,
        serdes: SerDes | None,
        summary_generator: SummaryGenerator | None = None,
        item_serdes: SerDes | None = None,
        nesting_type: NestingType = NestingType.NESTED,
    ):
        super().__init__(
            executables=executables,
            max_concurrency=max_concurrency,
            completion_config=completion_config,
            sub_type_top=top_level_sub_type,
            sub_type_iteration=iteration_sub_type,
            name_prefix=name_prefix,
            serdes=serdes,
            summary_generator=summary_generator,
            item_serdes=item_serdes,
            nesting_type=nesting_type,
        )

    @classmethod
    def from_callables(
        cls,
        callables: Sequence[Callable[[], Awaitable[R]] | ParallelBranch[R]],
        config: ParallelConfig,
    ) -> ParallelExecutor:
        """Create ParallelExecutor from a sequence of callables or ParallelBranch instances.

        Since ParallelBranch is callable, it is stored directly as the func in
        each Executable. The get_iteration_name method inspects the func to
        extract the branch name when available.
        """
        executables: list[Executable[Callable]] = [
            Executable(index=i, func=func) for i, func in enumerate(callables)
        ]

        return cls(
            executables=executables,
            max_concurrency=config.max_concurrency,
            completion_config=config.completion_config,
            top_level_sub_type=OperationSubType.PARALLEL,
            iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
            name_prefix="parallel-branch-",
            serdes=config.serdes,
            summary_generator=config.summary_generator,
            item_serdes=config.item_serdes,
            nesting_type=config.nesting_type,
        )

    def get_iteration_name(self, index: int) -> str:
        """Return custom branch name if the callable is a ParallelBranch with a name."""
        func = self.executables[index].func
        if isinstance(func, ParallelBranch) and func.name is not None:
            return func.name
        return super().get_iteration_name(index)

    async def execute_item(self, child_context, executable: Executable[Callable]):  # noqa: PLR6301
        logger.debug("🔀 Processing parallel branch: %s", executable.index)
        target = (
            executable.func.func
            if isinstance(executable.func, ParallelBranch)
            else executable.func
        )
        if getattr(child_context, "execution_state", None) is not None:
            result: R = await invoke_user_callable(
                child_context,
                target,
            )
        else:
            result = await invoke_callable(
                target,
                child_context,
            )
        logger.debug("✅ Processed parallel branch: %s", executable.index)
        return result


async def parallel_handler(
    callables: Sequence[Callable[[], Awaitable[R]] | ParallelBranch[R]],
    config: ParallelConfig | None,
    execution_state: ExecutionState,
    parallel_context: DurableContext,
    operation_identifier: OperationIdentifier,
):
    """Execute multiple operations in parallel."""
    # Summary Generator Construction (matches TypeScript implementation):
    # Construct the summary generator at the handler level, just like TypeScript does in parallel-handler.ts.
    # This matches the pattern where handlers are responsible for configuring operation-specific behavior.
    #
    # See TypeScript reference: aws-durable-execution-sdk-js/src/handlers/parallel-handler/parallel-handler.ts (~line 112)

    executor = ParallelExecutor.from_callables(
        callables,
        config or ParallelConfig(summary_generator=ParallelSummaryGenerator()),
    )

    checkpoint = get_checkpoint_result(
        execution_state,
        operation_identifier.require_operation_id(),
    )
    if checkpoint.is_succeeded():
        return await executor.replay(execution_state, parallel_context)
    return await executor.execute(execution_state, executor_context=parallel_context)


class ParallelSummaryGenerator:
    def __call__(self, result: BatchResult) -> str:
        fields = {
            "totalCount": result.total_count,
            "successCount": result.success_count,
            "failureCount": result.failure_count,
            "completionReason": result.completion_reason.value,
            "status": result.status.value,
            "startedCount": result.started_count,
            "type": "ParallelResult",
        }

        return json.dumps(fields)


async def parallel(
    functions: Sequence[Callable[[], Awaitable[T]] | ParallelBranch[T]],
    name: str | None = None,
    config: ParallelConfig | None = None,
):
    context = _get_durable_context("parallel")
    for index, function in enumerate(functions):
        target = function.func if isinstance(function, ParallelBranch) else function
        assert_async_callable(target, label=f"functions[{index}]")

    operation_id = context.step_counter.create_step_id()
    parallel_context = context.create_child_context(operation_id=operation_id)
    operation_identifier = OperationIdentifier(
        operation_id=operation_id,
        sub_type=OperationSubType.PARALLEL,
        parent_id=context.parent_id,
        name=name,
    )

    async def parallel_in_child_context() -> BatchResult[T]:
        return await parallel_handler(
            callables=functions,
            config=config,
            execution_state=context.execution_state,
            parallel_context=parallel_context,
            operation_identifier=operation_identifier,
        )

    result = await child_handler(
        func=parallel_in_child_context,
        state=context.execution_state,
        operation_identifier=operation_identifier,
        config=ChildConfig(
            serdes=getattr(config, "serdes", None),
            item_serdes=None,
        ),
    )
    context.execution_state.track_replay(operation_id=operation_id)
    return result


def durable_parallel_branch(
    name: str | None = None,
) -> Callable[
    [Callable[Params, Awaitable[T]]],
    Callable[Params, ParallelBranch[T]],
]:
    """Wrap your callable into a named ParallelBranch for use with `parallel()`.

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

            return await step(load_user, name="load_user")

        @durable_parallel_branch(name="fetch-orders")
        async def fetch_orders(user_id: str) -> list:
            ctx = get_context()

            async def load_orders() -> list:
                return ["order1", "order2"]

            return await step(load_orders, name="load_orders")

        # Usage in a durable handler:
        results = await parallel(
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
                from ..context import get_current_context

                current_context = (
                    runtime_args[0] if runtime_args else get_current_context()
                )
                if runtime_kwargs:
                    msg = "Parallel branches do not accept runtime keyword arguments."
                    raise TypeError(msg)
                return await invoke_callable(
                    func,
                    current_context,
                    *args,
                    **kwargs,
                )

            return ParallelBranch(func=function_with_arguments, name=name)

        return wrapper

    return decorator

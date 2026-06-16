"""Implementation for Durable Parallel operation."""

from __future__ import annotations
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, TypeVar

from ..async_tools import invoke_callable_with_optional_context
from async_durable_execution.operation.concurrency import ConcurrentExecutor
from ..config import (
    NestingType,
    ParallelBranch,
    ParallelConfig,
)
from ..models import Executable, OperationSubType


if TYPE_CHECKING:
    from ..context import DurableContext
    from ..models import BatchResult, OperationIdentifier
    from ..serdes import SerDes
    from ..state import ExecutionState
    from ..types import SummaryGenerator

logger = logging.getLogger(__name__)

# Result type
R = TypeVar("R")


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

    def execute_item(self, child_context, executable: Executable[Callable]):  # noqa: PLR6301
        awaitable = self._execute_item_async(child_context, executable)
        return awaitable

    async def _execute_item_async(
        self, child_context, executable: Executable[Callable]
    ) -> R:
        logger.debug("🔀 Processing parallel branch: %s", executable.index)
        target = (
            executable.func.func
            if isinstance(executable.func, ParallelBranch)
            else executable.func
        )
        invoke_with_context = getattr(
            type(child_context), "_invoke_user_callable", None
        )
        if invoke_with_context is not None:
            result: R = await child_context._invoke_user_callable(
                target,
                context_position="prepend",
            )
        else:
            result = await invoke_callable_with_optional_context(
                target,
                child_context,
                context_position="prepend",
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

    checkpoint = execution_state.get_checkpoint_result(
        operation_identifier.operation_id
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

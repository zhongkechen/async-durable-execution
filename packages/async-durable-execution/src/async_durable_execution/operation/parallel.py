"""Implementation for Durable Parallel operation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    TypeVar,
    Sequence,
    Callable,
    Awaitable,
)

from .base import get_checkpoint_result
from .child import ChildConfig, child_handler, _get_durable_context

from ..async_tools import (
    invoke_user_callable,
    invoke_callable,
    assert_async_callable,
)
from async_durable_execution.operation.concurrency import (
    CompletionConfig,
    ConcurrentExecutor,
    NestingType,
)
from ..models import Executable, OperationIdentifier, OperationSubType


if TYPE_CHECKING:
    from ..models import BatchResult
    from ..serdes import SerDes
    from ..state import ExecutionState
    from ..types import SummaryGenerator
    from .child import DurableContext

logger = logging.getLogger(__name__)

# Result type
R = TypeVar("R")
T = TypeVar("T")


@dataclass(frozen=True)
class ParallelConfig:
    """Configuration options for parallel execution operations."""

    max_concurrency: int | None = None
    completion_config: CompletionConfig = field(
        default_factory=CompletionConfig.all_successful
    )
    serdes: SerDes | None = None
    item_serdes: SerDes | None = None
    summary_generator: SummaryGenerator | None = None
    nesting_type: NestingType = NestingType.NESTED


class ParallelExecutor(ConcurrentExecutor[Callable, R]):
    """Concurrent executor used by the public `parallel()` helper."""

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
        callables: Sequence[Callable[[], Awaitable[R]]],
        config: ParallelConfig,
    ) -> ParallelExecutor:
        """Create ParallelExecutor from a sequence of bound durable callables."""
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

    async def execute_item(self, child_context, executable: Executable[Callable]):  # noqa: PLR6301
        logger.debug("🔀 Processing parallel branch: %s", executable.index)
        if getattr(child_context, "execution_state", None) is not None:
            result: R = await invoke_user_callable(
                child_context,
                executable.func,
            )
        else:
            result = await invoke_callable(
                executable.func,
            )
        logger.debug("✅ Processed parallel branch: %s", executable.index)
        return result


async def parallel_handler(
    callables: Sequence[Callable[[], Awaitable[R]]],
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
    """Default summary generator for oversized parallel `BatchResult` payloads."""

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
    functions: Sequence[Callable[[], Awaitable[T]]],
    name: str | None = None,
    config: ParallelConfig | None = None,
):
    """Run multiple bound durable callables concurrently and return a `BatchResult`."""
    context = _get_durable_context("parallel")
    for index, function in enumerate(functions):
        assert_async_callable(function, label=f"functions[{index}]")

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

"""Implementation for Durable Map operation."""

from __future__ import annotations

import json
import inspect
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Generic, TypeVar

from async_durable_execution.async_tools import (
    invoke_callable,
    run_or_return,
)
from async_durable_execution.concurrency.executor import ConcurrentExecutor
from async_durable_execution.concurrency.models import (
    BatchResult,
    Executable,
)
from async_durable_execution.config import MapConfig, NestingType
from async_durable_execution.lambda_service import OperationSubType


if TYPE_CHECKING:
    from async_durable_execution.context import DurableContext
    from async_durable_execution.identifier import OperationIdentifier
    from async_durable_execution.serdes import SerDes
    from async_durable_execution.state import (
        CheckpointedResult,
        ExecutionState,
    )
    from async_durable_execution.types import SummaryGenerator

logger = logging.getLogger(__name__)

# Input item type
T = TypeVar("T")
# Result type
R = TypeVar("R")


class MapExecutor(Generic[T, R], ConcurrentExecutor[Callable, R]):  # noqa: PYI059
    def __init__(
        self,
        executables: list[Executable[Callable]],
        items: Sequence[T],
        max_concurrency: int | None,
        completion_config,
        top_level_sub_type: OperationSubType,
        iteration_sub_type: OperationSubType,
        name_prefix: str,
        serdes: SerDes | None,
        summary_generator: SummaryGenerator | None = None,
        item_serdes: SerDes | None = None,
        nesting_type: NestingType = NestingType.NESTED,
        item_namer: Callable[[T, int], str] | None = None,
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
        self.items = items
        self._item_namer = item_namer

    @classmethod
    def from_items(
        cls,
        items: Sequence[T],
        func: Callable[[DurableContext, T, int, Sequence[T]], Awaitable[R]],
        config: MapConfig[T],
    ) -> MapExecutor[T, R]:
        """Create MapExecutor from items and a callable."""
        executables: list[Executable[Callable]] = [
            Executable(index=i, func=func) for i in range(len(items))
        ]

        return cls(
            executables=executables,
            items=items,
            max_concurrency=config.max_concurrency,
            completion_config=config.completion_config,
            top_level_sub_type=OperationSubType.MAP,
            iteration_sub_type=OperationSubType.MAP_ITERATION,
            name_prefix="map-item-",
            serdes=config.serdes,
            summary_generator=config.summary_generator,
            item_serdes=config.item_serdes,
            nesting_type=config.nesting_type,
            item_namer=config.item_namer,
        )

    def get_iteration_name(self, index: int) -> str:
        """Return custom item name if item_namer is provided, otherwise default."""
        if self._item_namer is not None:
            return self._item_namer(self.items[index], index)
        return super().get_iteration_name(index)

    def execute_item(self, child_context, executable: Executable[Callable]):
        return run_or_return(self._execute_item_async(child_context, executable))

    async def _execute_item_async(
        self, child_context, executable: Executable[Callable]
    ) -> R:
        logger.debug("🗺️ Processing map item: %s", executable.index)
        item = self.items[executable.index]
        result: R = await invoke_callable(
            executable.func, child_context, item, executable.index, self.items
        )
        logger.debug("✅ Processed map item: %s", executable.index)
        return result


def map_handler(
    items: Sequence[T],
    func: Callable[[DurableContext, T, int, Sequence[T]], Awaitable[R]],
    config: MapConfig | None,
    execution_state: ExecutionState,
    map_context: DurableContext,
    operation_identifier: OperationIdentifier,
):
    return run_or_return(
        _map_handler_async(
            items,
            func,
            config,
            execution_state,
            map_context,
            operation_identifier,
        )
    )


async def _map_handler_async(
    items: Sequence[T],
    func: Callable[[DurableContext, T, int, Sequence[T]], Awaitable[R]],
    config: MapConfig | None,
    execution_state: ExecutionState,
    map_context: DurableContext,
    operation_identifier: OperationIdentifier,
) -> BatchResult[R]:
    """Execute a callable for each item in parallel."""
    # Summary Generator Construction (matches TypeScript implementation):
    # Construct the summary generator at the handler level, just like TypeScript does in map-handler.ts.
    # This matches the pattern where handlers are responsible for configuring operation-specific behavior.
    #
    # See TypeScript reference: aws-durable-execution-sdk-js/src/handlers/map-handler/map-handler.ts (~line 79)

    executor: MapExecutor[T, R] = MapExecutor.from_items(
        items=items,
        func=func,
        config=config or MapConfig(summary_generator=MapSummaryGenerator()),
    )

    checkpoint: CheckpointedResult = execution_state.get_checkpoint_result(
        operation_identifier.operation_id
    )
    if checkpoint.is_succeeded():
        # if we've reached this point, then not only is the step succeeded, but it is also `replay_children`.
        replay_result = executor.replay(execution_state, map_context)
        if inspect.isawaitable(replay_result):
            return await replay_result
        return replay_result
    # we are making it explicit that we are now executing within the map_context
    execute_result = executor.execute(execution_state, executor_context=map_context)
    if inspect.isawaitable(execute_result):
        return await execute_result
    return execute_result


class MapSummaryGenerator:
    def __call__(self, result: BatchResult) -> str:
        fields = {
            "totalCount": result.total_count,
            "successCount": result.success_count,
            "failureCount": result.failure_count,
            "completionReason": result.completion_reason.value,
            "status": result.status.value,
            "type": "MapResult",
        }
        return json.dumps(fields)

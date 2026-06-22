"""Implementation for Durable Map operation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar, Sequence, Callable, Any, Awaitable

from ..async_tools import get_callable_name
from .base import CheckpointedResult, get_checkpoint_result
from .child import ChildConfig, DurableContext, child_handler, _get_durable_context

from ..async_tools import (
    invoke_user_callable,
    assert_async_callable,
)
from async_durable_execution.operation.concurrency import (
    CompletionConfig,
    ConcurrentExecutor,
    NestingType,
)
from ..models import BatchResult, Executable, OperationIdentifier, OperationSubType


if TYPE_CHECKING:
    from ..serdes import SerDes
    from ..state import ExecutionState
    from ..types import SummaryGenerator

logger = logging.getLogger(__name__)

# Input item type
T = TypeVar("T")
# Result type
R = TypeVar("R")
U = TypeVar("U")


@dataclass(frozen=True)
class BatchedInput(Generic[T, U]):
    """Wrapper passed to batched map handlers."""

    batch_input: T
    items: list[U]


@dataclass(frozen=True)
class ItemBatcher(Generic[T]):
    """Configuration for batching items in map operations."""

    max_items_per_batch: int = 0
    max_item_bytes_per_batch: int | float = 0
    batch_input: T | None = None


@dataclass(frozen=True)
class MapConfig(Generic[T]):
    """Configuration options for map operations over collections."""

    max_concurrency: int | None = None
    item_batcher: ItemBatcher = field(default_factory=ItemBatcher)
    completion_config: CompletionConfig = field(default_factory=CompletionConfig)
    serdes: SerDes | None = None
    item_serdes: SerDes | None = None
    summary_generator: SummaryGenerator | None = None
    nesting_type: NestingType = NestingType.NESTED
    item_namer: Callable[[T, int], str] | None = None


@dataclass(frozen=True)
class MapItemContext(DurableContext, Generic[T]):
    """Context exposed while a map item function is executing."""

    index: int = 0
    items: Sequence[T] = field(default_factory=tuple)


class MapExecutor(Generic[T, R], ConcurrentExecutor[Callable, R]):  # noqa: PYI059
    """Concurrent executor used by the public `map()` helper."""

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
        func: Callable[[T], Awaitable[R]],
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

    async def execute_item(self, child_context, executable: Executable[Callable]):
        logger.debug("🗺️ Processing map item: %s", executable.index)
        item = self.items[executable.index]
        map_item_context = MapItemContext(
            execution_state=child_context.execution_state,
            operation_identifier=child_context.operation_identifier,
            lambda_context=child_context.lambda_context,
            step_id_prefix=child_context.step_id_prefix,
            index=executable.index,
            items=self.items,
        )
        result: R = await invoke_user_callable(
            map_item_context,
            executable.func,
            item,
        )
        logger.debug("✅ Processed map item: %s", executable.index)
        return result


async def map_handler(
    items: Sequence[T],
    func: Callable[[T], Awaitable[R]],
    config: MapConfig | None,
    execution_state: ExecutionState,
    map_context: DurableContext,
    operation_identifier: OperationIdentifier,
):
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

    checkpoint: CheckpointedResult = get_checkpoint_result(
        execution_state,
        operation_identifier.require_operation_id(),
    )
    if checkpoint.is_succeeded():
        # if we've reached this point, then not only is the step succeeded, but it is also `replay_children`.
        return await executor.replay(execution_state, map_context)
    # we are making it explicit that we are now executing within the map_context
    return await executor.execute(execution_state, executor_context=map_context)


class MapSummaryGenerator:
    """Default summary generator for oversized `BatchResult` map payloads."""

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


async def map(
    inputs: Sequence[U],
    func: Callable[[U | BatchedInput[Any, U]], Awaitable[T]],
    name: str | None = None,
    config: MapConfig | None = None,
):
    """Process a collection durably with optional concurrency and batching controls."""
    context = _get_durable_context("map")
    assert_async_callable(func)
    map_name: str | None = name or get_callable_name(func)

    operation_id = context.step_counter.create_step_id()
    operation_identifier = OperationIdentifier(
        operation_id=operation_id,
        sub_type=OperationSubType.MAP,
        parent_id=context.parent_id,
        name=map_name,
    )
    map_context = context.create_child_context(operation_id=operation_id)

    async def map_in_child_context() -> BatchResult[T]:
        return await map_handler(
            items=inputs,
            func=func,
            config=config,
            execution_state=context.execution_state,
            map_context=map_context,
            operation_identifier=operation_identifier,
        )

    result = await child_handler(
        func=map_in_child_context,
        state=context.execution_state,
        operation_identifier=operation_identifier,
        config=ChildConfig(
            serdes=getattr(config, "serdes", None),
            item_serdes=None,
        ),
    )
    context.execution_state.track_replay(operation_id=operation_id)
    return result

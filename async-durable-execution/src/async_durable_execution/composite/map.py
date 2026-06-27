"""Implementation for Durable Map operation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Generic,
    TypeVar,
    Sequence,
    Iterable,
    Callable,
    Any,
    Awaitable,
)

from ..primitive.base import CheckpointedResult, get_checkpoint_result
from ..primitive.child import (
    DurableContext,
    child_handler,
    _get_durable_context,
)

from ..async_tools import (
    invoke_user_callable,
    assert_async_callable,
    durable_callable,
)
from .concurrency import (
    BatchResult,
    CompletionConfig,
    ConcurrentExecutor,
    Executable,
    NestingType,
)
from ..models import OperationIdentifier, OperationSubType


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


@durable_callable
async def map_handler(
    items: Sequence[T],
    func: Callable[[T], Awaitable[R]],
    execution_state: ExecutionState,
    map_context: DurableContext,
    operation_identifier: OperationIdentifier,
    *,
    max_concurrency: int | None = None,
    completion_config: CompletionConfig | None = None,
    serdes: SerDes | None = None,
    item_serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    nesting_type: NestingType = NestingType.NESTED,
    item_namer: Callable[[T, int], str] | None = None,
):
    """Execute a callable for each item in parallel."""
    executor: MapExecutor[T, R] = MapExecutor(
        executables=[Executable(index=i, func=func) for i in range(len(items))],
        items=items,
        max_concurrency=max_concurrency,
        completion_config=completion_config or CompletionConfig(),
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        serdes=serdes,
        summary_generator=summary_generator,
        item_serdes=item_serdes,
        nesting_type=nesting_type,
        item_namer=item_namer,
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


async def map(
    func: Callable[[U | BatchedInput[Any, U]], Awaitable[T]],
    items: Iterable[U],
    *,
    name: str | None = None,
    max_concurrency: int | None = None,
    item_batcher: ItemBatcher | None = None,
    completion_config: CompletionConfig | None = None,
    serdes: SerDes | None = None,
    item_serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    nesting_type: NestingType = NestingType.NESTED,
    item_namer: Callable[[U, int], str] | None = None,
):
    """Process a collection durably with optional concurrency and batching controls.

    Args:
        func: Async callable that processes each item.
        items: Items to process.
        name: Optional durable operation name.
        max_concurrency: Optional limit for concurrent item processing.
        item_batcher: Optional item batching configuration.
        completion_config: Optional completion criteria.
        serdes: Optional serializer for the map result.
        item_serdes: Optional serializer for individual map item results.
        summary_generator: Optional summary generator for large map results.
        nesting_type: Whether map iterations use nested or flat operation ids.
        item_namer: Optional callable for naming map item iterations.
    """
    context = _get_durable_context("map")
    assert_async_callable(func)
    items_sequence = list(items)
    map_name = name if name is not None else getattr(func, "__name__", None)
    with context._replay_aware():
        operation_id = context.step_counter.create_step_id()
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.MAP,
            parent_id=context.parent_id,
            name=map_name,
        )
        map_context = context.create_child_context(operation_id=operation_id)

        return await child_handler(
            func=map_handler(
                items=items_sequence,
                func=func,
                execution_state=context.execution_state,
                map_context=map_context,
                operation_identifier=operation_identifier,
                max_concurrency=max_concurrency,
                completion_config=completion_config,
                serdes=serdes,
                item_serdes=item_serdes,
                summary_generator=summary_generator,
                nesting_type=nesting_type,
                item_namer=item_namer,
            ),
            state=context.execution_state,
            operation_identifier=operation_identifier,
            serdes=serdes,
            item_serdes=None,
        )

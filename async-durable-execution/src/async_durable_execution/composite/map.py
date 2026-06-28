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

from .parallel import (
    BatchResult,
    CompletionConfig,
    NestingType,
)
from .parallel import parallel_handler
from ..context import bind_current_context
from ..execution import durable_callable
from ..models import OperationIdentifier, OperationSubType
from ..primitive.child import (
    DurableContext,
    _run_in_child_context,
    get_durable_context,
)

if TYPE_CHECKING:
    from .parallel import SummaryGenerator
    from ..serdes import SerDes
    from ..state import ExecutionState

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
class MapItemContext(DurableContext, Generic[T]):
    """Context exposed while a map item function is executing."""

    index: int = 0
    items: Sequence[T] = field(default_factory=tuple)


def _bind_map_item_to_branch(
    items: Sequence[T],
    index: int,
    func: Callable[[T], Awaitable[R]],
) -> Callable[[], Awaitable[R]]:
    async def run_branch() -> R:
        logger.debug("🗺️ Processing map item: %s", index)
        item = items[index]
        child_context = get_durable_context("map")
        map_item_context = MapItemContext(
            execution_state=child_context.execution_state,
            operation_identifier=child_context.operation_identifier,
            step_id_prefix=child_context.step_id_prefix,
            replaying=child_context.is_replaying(),
            index=index,
            items=items,
        )
        with bind_current_context(map_item_context):
            result: R = await func(item)
        logger.debug("✅ Processed map item: %s", index)
        return result

    return run_branch


def _create_map_branches(
    items: Sequence[T],
    func: Callable[[T], Awaitable[R]],
) -> list[Callable[[], Awaitable[R]]]:
    return [
        _bind_map_item_to_branch(items=items, index=index, func=func)
        for index in range(len(items))
    ]


def _create_map_branch_namer(
    items: Sequence[T],
    item_namer: Callable[[T, int], str] | None,
) -> Callable[[int], str] | None:
    if item_namer is None:
        return None

    def name_branch(index: int) -> str:
        return item_namer(items[index], index)

    return name_branch


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
    summary_generator: SummaryGenerator | None = MapSummaryGenerator(),
    nesting_type: NestingType = NestingType.NESTED,
    item_namer: Callable[[T, int], str] | None = None,
):
    """Execute a callable for each item through the parallel handler."""
    handler = parallel_handler(
        callables=_create_map_branches(items, func),
        max_concurrency=max_concurrency,
        completion_config=completion_config or CompletionConfig(),
        serdes=serdes,
        summary_generator=summary_generator,
        item_serdes=item_serdes,
        nesting_type=nesting_type,
        execution_state=execution_state,
        parallel_context=map_context,
        operation_identifier=operation_identifier,
        top_level_sub_type=OperationSubType.MAP,
        iteration_sub_type=OperationSubType.MAP_ITERATION,
        name_prefix="map-item-",
        branch_namer=_create_map_branch_namer(items, item_namer),
    )

    return await handler()


async def map(
    func: Callable[[U | BatchedInput[Any, U]], Awaitable[T]],
    items: Iterable[U],
    *,
    name: str | None = None,
    max_concurrency: int | None = None,
    completion_config: CompletionConfig | None = None,
    serdes: SerDes | None = None,
    item_serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = MapSummaryGenerator(),
    nesting_type: NestingType = NestingType.NESTED,
    item_namer: Callable[[U, int], str] | None = None,
):
    """Process a collection durably with optional concurrency controls.

    Args:
        func: Async callable that processes each item.
        items: Items to process.
        name: Optional durable operation name.
        max_concurrency: Optional limit for concurrent item processing.
        completion_config: Optional completion criteria.
        serdes: Optional serializer for the map result.
        item_serdes: Optional serializer for individual map item results.
        summary_generator: Optional summary generator for large map results.
        nesting_type: Whether map iterations use nested or flat operation ids.
        item_namer: Optional callable for naming map item iterations.
    """
    context = get_durable_context("map")
    items_sequence = list(items)
    map_name = name if name is not None else getattr(func, "__name__", None)

    async def run_map_handler() -> BatchResult[T]:
        map_context = get_durable_context("map")
        operation_id = map_context.step_id_prefix
        if operation_id is None:
            msg = "map operation id is not available in the current context"
            raise RuntimeError(msg)
        operation_identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.MAP,
            parent_id=map_context.parent_id,
            name=map_name,
        )

        handler = map_handler(
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
        )
        return await handler()

    return await _run_in_child_context(
        run_map_handler,
        sub_type=OperationSubType.MAP,
        name=map_name,
        serdes=serdes,
    )

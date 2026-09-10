"""Implementation for Durable Map operation."""

from __future__ import annotations

from .parallel import _FlatReplaySummary

import asyncio
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
    _BATCH_RESULT_SERDES,
    BatchResult,
    CompletionConfig,
    NestingType,
    _validate_max_concurrency,
)
from .parallel import parallel_handler
from .._core import (
    DurableContext,
    ExecutionState,
    OperationIdentifier,
    OperationSubType,
    SerDes,
    bind_current_context,
    durable_callable,
    get_current_context,
    get_durable_context,
)
from .._extension_api import get_extension_context

if TYPE_CHECKING:
    from .child import SummaryGenerator

logger = logging.getLogger(__name__)

# Input item type
T = TypeVar("T")
# Result type
R = TypeVar("R")
U = TypeVar("U")


def _run_in_child_context(
    func: Callable[[], Awaitable[T]],
    *,
    sub_type: OperationSubType,
    name: str | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[T]:
    """Run an SDK-owned child operation through the stable operation SPI."""
    return (
        get_extension_context()
        ._reserve_sdk_operation(name)  # noqa: SLF001
        ._run_in_child_context(  # noqa: SLF001
            func,
            sub_type=sub_type,
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
        )
    )


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


def get_map_item_context() -> MapItemContext[Any]:
    """Return the active `MapItemContext`."""
    current_context = get_current_context()
    if not isinstance(current_context, MapItemContext):
        msg = (
            "get_map_item_context() can only be used while a map item "
            "function is executing."
        )
        raise RuntimeError(msg)
    return current_context


def _bind_map_item_to_branch(
    items: Sequence[T],
    index: int,
    func: Callable[[T], Awaitable[R]],
) -> Callable[[], Awaitable[R]]:
    async def run_branch() -> R:
        logger.debug("🗺️ Processing map item: %s", index)
        item = items[index]
        child_context = get_durable_context()
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
) -> BatchResult[R]:
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


def map(
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
) -> asyncio.Task[BatchResult[T]]:
    """Start a durable map operation over a collection of items.

    `map()` creates one durable child context per item and calls `func` with that
    item. The item function must be async and may contain durable operations such
    as `step()` or `wait()`.

    The returned object is an `asyncio.Task`; awaiting it yields a `BatchResult`.
    Calling `map()` without immediately awaiting it schedules the durable
    operation in the background, consistent with other operation helpers.

    By default, `map()` uses `CompletionConfig()` with no explicit success
    threshold or failure tolerance: all-successful completion produces
    `CompletionReason.ALL_COMPLETED`, while any observed failure completes the
    operation as failed. Pass `completion_config` to use threshold-based or
    custom completion.

    Args:
        func: Async callable that processes each item. It receives the original
            item value and returns that item's result.
        items: Items to process.
        name: Optional durable operation name.
        max_concurrency: Optional limit for in-flight items. A suspended item
            retains its slot until it reaches a terminal state.
        completion_config: Optional completion policy. Use
            `CompletionConfig.thresholds()`, `first_successful()`,
            `all_completed()`, `all_successful()`, or `custom()`.
        serdes: Optional serializer for the final `BatchResult`.
        item_serdes: Optional serializer for each item result.
        summary_generator: Optional callable used to summarize oversized
            checkpoint payloads.
        nesting_type: Whether map iterations use nested or flat operation
            identifiers.
        item_namer: Optional callable for naming map item iterations.

    Returns:
        An `asyncio.Task` that resolves to a `BatchResult` containing one
        `BatchItem` per input item.

    Raises:
        RuntimeError: If called outside a durable context.
    """
    _validate_max_concurrency(max_concurrency)
    get_durable_context()
    items_sequence = list(items)
    map_name = name if name is not None else getattr(func, "__name__", None)

    async def run_map_handler() -> BatchResult[T]:
        map_context = get_durable_context()
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
            execution_state=map_context.execution_state,
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

    summary_options: dict[str, Any] = {}
    if nesting_type is NestingType.FLAT:
        summary_options["summary_generator"] = _FlatReplaySummary(summary_generator)
    return _run_in_child_context(
        run_map_handler,
        sub_type=OperationSubType.MAP,
        name=map_name,
        serdes=serdes if serdes is not None else _BATCH_RESULT_SERDES,
        **summary_options,
    )

"""Concurrent groups collect outcomes once and persist the complete decision."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable, Iterable, TypeVar
from ._serde import SerDes

T = TypeVar("T")
U = TypeVar("U")

from ._effects import reserve, scope_effect
from ._journal import Pause, cursor
from ._scope import MapItemContext, get_durable_context
from ._types import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionConfig,
    CompletionReason,
    CompletionStatus,
    ErrorObject,
    InvalidStateError,
    InvocationError,
    NestingType,
    OperationSubType,
    SerDesError,
    ValidationError,
)


class BatchPreview:
    def __init__(self, kind):
        self.kind = kind

    def __call__(self, result):
        if not isinstance(result, BatchResult):
            return json.dumps({"type": self.kind, "valueType": type(result).__name__})
        return json.dumps(
            {
                "type": self.kind,
                "totalCount": result.total_count,
                "successCount": result.success_count,
                "failureCount": result.failure_count,
                "completionReason": result.completion_reason.value,
            }
        )


_parallel_preview = BatchPreview("ParallelResult")
_map_preview = BatchPreview("MapResult")


def limit(value):
    if value is not None and (type(value) is not int or value < 1):
        raise ValidationError("max_concurrency must be a positive integer or None")


async def collect(
    functions,
    *,
    max_concurrency,
    policy,
    item_serdes,
    summary_generator,
    nesting_type,
    names,
    map_items=None,
):
    count = len(functions)
    if not count:
        return BatchResult([], CompletionReason.ALL_COMPLETED)
    namespace = cursor(get_durable_context())
    tickets = [namespace.reserve(names(i), f"branch-{i}") for i in range(count)]
    active: dict[asyncio.Task, int] = {}
    outcomes: dict[int, BatchItem] = {}
    paused: dict[int, Pause] = {}
    launched = 0
    decision = None

    def launch(index):
        ticket = tickets[index].select(
            "CONTEXT",
            OperationSubType.MAP_ITERATION
            if map_items is not None
            else OperationSubType.PARALLEL_BRANCH,
        )
        context = (
            ticket.child(
                virtual=nesting_type is NestingType.FLAT,
                kind=MapItemContext,
                index=index,
                items=map_items,
            )
            if map_items is not None
            else None
        )
        task = ticket.spawn(
            scope_effect(
                ticket,
                functions[index],
                serdes=item_serdes,
                summary_generator=summary_generator,
                is_virtual=nesting_type is NestingType.FLAT,
                context=context,
            )
        )
        active[task] = index

    def status():
        return CompletionStatus(
            sum(x.status is BatchItemStatus.SUCCEEDED for x in outcomes.values()),
            sum(x.status is BatchItemStatus.FAILED for x in outcomes.values()),
            count,
        )

    def observe(task, index):
        if task.cancelled():
            raise asyncio.CancelledError()
        try:
            value = task.result()
        except Pause as pause:
            paused[index] = pause
        except (InvocationError, SerDesError):
            raise
        except Exception as error:
            outcomes[index] = BatchItem(
                index, BatchItemStatus.FAILED, error=ErrorObject.from_exception(error)
            )
        else:
            outcomes[index] = BatchItem(index, BatchItemStatus.SUCCEEDED, value)

    try:
        while launched < min(count, max_concurrency or count):
            launch(launched)
            launched += 1
        while active:
            completed, _ = await asyncio.wait(
                active, return_when=asyncio.FIRST_COMPLETED
            )
            for task in sorted(completed, key=lambda task: active[task]):
                index = active.pop(task)
                observe(task, index)
                decision = policy.completion_decision(status())
                if decision.should_complete:
                    break
                if index in outcomes and launched < count:
                    launch(launched)
                    launched += 1
            if decision and decision.should_complete:
                break
        if not decision or not decision.should_complete:
            if paused:
                deadlines = [p.until for p in paused.values() if p.until is not None]
                raise Pause(min(deadlines) if deadlines else None)
            raise InvalidStateError(
                "Completion policy did not finish after all branches settled"
            )
    finally:
        for task in active:
            if not task.done():
                task.cancel()
        await asyncio.gather(*active, return_exceptions=True)
    # Persist every settled result and every cancellation. The parent stores this
    # value (in chunks when necessary), so terminal replay never calls collect().
    for task, index in active.items():
        if task.cancelled() or isinstance(task.exception(), Pause):
            outcomes[index] = BatchItem(index, BatchItemStatus.CANCELLED)
        elif task.exception() is not None:
            outcomes[index] = BatchItem(
                index,
                BatchItemStatus.FAILED,
                error=ErrorObject.from_exception(task.exception()),
            )
        else:
            outcomes[index] = BatchItem(index, BatchItemStatus.SUCCEEDED, task.result())
    for index in paused:
        outcomes[index] = BatchItem(index, BatchItemStatus.CANCELLED)
    return BatchResult(
        [outcomes[i] for i in sorted(outcomes)], decision.completion_reason
    )


def parallel(
    branches: Iterable[Callable[[], Awaitable[T]]],
    *,
    name: str | None = None,
    max_concurrency: int | None = None,
    completion_config: CompletionConfig | None = None,
    serdes: SerDes | None = None,
    item_serdes: SerDes | None = None,
    summary_generator: Callable | None = _parallel_preview,
    nesting_type: NestingType = NestingType.NESTED,
) -> asyncio.Task[BatchResult[T]]:
    """Run bound async branches with durable results and bounded concurrency.

    A suspended branch retains its concurrency slot. Early completion cancels
    started unfinished branches and records their cancellation; branches that
    never started are omitted. Completed aggregate replay restores the same result
    without restarting cancelled work, including when the result is oversized.

    Args:
        branches: Iterable of zero-argument async callables, materialized at the call
            site.
        name: Optional stable aggregate name.
        max_concurrency: Positive running-branch limit; None starts all branches.
        completion_config: Completion policy; defaults to all_successful().
        serdes: Codec for the combined BatchResult and fallback codec for branch
            results.
        item_serdes: Branch result codec, overriding serdes for individual branches.
        summary_generator: Optional preparation callback for oversized branch results.
        nesting_type: NESTED persists branch contexts; FLAT keeps only isolated
            identities.

    Returns:
        (asyncio.Task[BatchResult[T]]): Recorded successes, failures, cancellations, and
            reason.

    Raises:
        ValidationError: max_concurrency is not a positive integer or None.
    """
    limit(max_concurrency)
    functions = list(branches)
    ticket = reserve("CONTEXT", OperationSubType.PARALLEL, name)

    async def body():
        return await collect(
            functions,
            max_concurrency=max_concurrency,
            policy=completion_config or CompletionConfig.all_successful(),
            item_serdes=item_serdes or serdes,
            summary_generator=summary_generator,
            nesting_type=nesting_type,
            names=lambda i: f"parallel-branch-{i}",
        )

    return ticket.spawn(scope_effect(ticket, body, serdes=serdes))


def map(
    func: Callable[[U], Awaitable[T]],
    items: Iterable[U],
    *,
    name: str | None = None,
    max_concurrency: int | None = None,
    completion_config: CompletionConfig | None = None,
    serdes: SerDes | None = None,
    item_serdes: SerDes | None = None,
    summary_generator: Callable | None = _map_preview,
    nesting_type: NestingType = NestingType.NESTED,
    item_namer: Callable[[U, int], str] | None = None,
) -> asyncio.Task[BatchResult[T]]:
    """Apply an async function to each input with durable branch results.

    The iterable is materialized once per workflow invocation. Each item runs in
    its own context, exposing its index and complete input sequence through
    get_map_item_context(). Suspended items retain their concurrency slots.
    Completed replay restores results and cancellations without rerunning items.

    Args:
        func: Async item function; it may compose durable operations.
        items: Iterable of inputs to materialize and process in original index order.
        name: Aggregate name; defaults to the function name when available.
        max_concurrency: Positive active-item limit; None starts all items.
        completion_config: Completion policy; None uses the default zero-failure policy.
        serdes: Combined BatchResult codec and fallback codec for item results.
        item_serdes: Item result codec, overriding the aggregate codec for each item.
        summary_generator: Optional preparation callback for oversized item results.
        nesting_type: Whether to persist separate item contexts or flatten their
            history.
        item_namer: Deterministic callback from item and index to a branch name.

    Returns:
        (asyncio.Task[BatchResult[T]]): Recorded outcomes; items never started are
            omitted.

    Raises:
        ValidationError: max_concurrency is not a positive integer or None.
    """
    limit(max_concurrency)
    values = list(items)
    functions = []
    for value in values:

        async def run(item=value):
            return await func(item)

        functions.append(run)
    ticket = reserve(
        "CONTEXT", OperationSubType.MAP, name or getattr(func, "__name__", None)
    )

    async def body():
        return await collect(
            functions,
            max_concurrency=max_concurrency,
            policy=completion_config or CompletionConfig(),
            item_serdes=item_serdes or serdes,
            summary_generator=summary_generator,
            nesting_type=nesting_type,
            names=lambda i: item_namer(values[i], i) if item_namer else f"map-item-{i}",
            map_items=values,
        )

    return ticket.spawn(scope_effect(ticket, body, serdes=serdes))

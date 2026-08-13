"""wait_for_callback extension built from callback, step, and child context."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .._core import (
    Duration,
    OperationContext,
    OperationSubType,
    SerDes,
    bind_current_context,
    durable_callable,
    get_current_context,
)
from ..extension import get_extension_context
from .callback import Callback, create_callback as _create_callback
from .step import get_step_context, step as _step

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from .child import SummaryGenerator

logger = logging.getLogger(__name__)


def create_callback(
    *,
    name: str | None = None,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes | None = None,
) -> asyncio.Task[Callback]:
    """Create an SDK-owned callback through the stable operation SPI."""
    return _create_callback(
        name=name,
        timeout=timeout,
        heartbeat_timeout=heartbeat_timeout,
        serdes=serdes,
    )


def step(
    func: Callable[[], Awaitable[Any]],
    *,
    name: str | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
    serdes: SerDes | None = None,
) -> asyncio.Task[Any]:
    """Run an SDK-owned submitter step through the stable operation SPI."""
    return _step(
        func,
        name=name,
        retry_strategy=retry_strategy,
        serdes=serdes,
    )


def _create_child_context_task(
    func: Callable[[], Awaitable[Any]],
    *,
    sub_type: OperationSubType,
    name: str | None = None,
    serdes: SerDes | None = None,
    summary_generator: SummaryGenerator | None = None,
    is_virtual: bool = False,
) -> asyncio.Task[Any]:
    """Run an SDK-owned callback scope through the stable operation SPI."""
    return (
        get_extension_context()
        .reserve(name)
        .run_in_child_context(
            func,
            sub_type=sub_type,
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
        )
    )


@durable_callable
async def wait_for_callback_handler(
    submitter: Callable[[], Awaitable[Any]],
    name: str | None = None,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
) -> Any:
    """Create a callback, run a submitter, and wait for callback completion."""
    callback_name = f"{name}-callback" if name is not None else "callback"
    submitter_step_name = f"{name}-submitter" if name is not None else "submitter"
    callback: Callback = await create_callback(
        name=callback_name,
        timeout=timeout,
        heartbeat_timeout=heartbeat_timeout,
        serdes=serdes,
    )

    async def submitter_step() -> Any:
        step_context = get_step_context()
        callback_context = WaitForCallbackContext(
            callback_id=callback.callback_id,
            execution_state=step_context.execution_state,
            operation_identifier=step_context.operation_identifier,
        )
        with bind_current_context(callback_context):
            return await submitter()

    await step(
        func=submitter_step,
        name=submitter_step_name,
        retry_strategy=retry_strategy,
        serdes=serdes,
    )

    return await callback.result()


def wait_for_callback(
    submitter: Callable[[], Awaitable[Any]],
    *,
    name: str | None = None,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes | None = None,
    retry_strategy: Callable[[Exception, int], Duration | None] | None = None,
) -> asyncio.Task[Any]:
    """Create a callback, run a submitter, then suspend until the callback resolves.

    Args:
        submitter: Async callable. Use get_wait_for_callback_context().callback_id
            inside the submitter to access the callback id.
        name: Optional durable operation name.
        timeout: Optional maximum time to wait for callback completion.
        heartbeat_timeout: Optional maximum time to wait between callback heartbeats.
        serdes: Optional serializer for callback results and submitter results.
        retry_strategy: Optional strategy that returns a retry delay or None to stop.
    """
    context_name = name if name is not None else getattr(submitter, "__name__", None)
    logger.debug("wait_for_callback name: %s", context_name)

    return _create_child_context_task(
        wait_for_callback_handler(
            submitter,
            name=context_name,
            timeout=timeout,
            heartbeat_timeout=heartbeat_timeout,
            serdes=serdes,
            retry_strategy=retry_strategy,
        ),
        sub_type=OperationSubType.WAIT_FOR_CALLBACK,
        name=context_name,
        serdes=serdes,
    )


@dataclass(frozen=True)
class WaitForCallbackContext(OperationContext):
    """Context available during wait_for_callback submitter execution."""

    callback_id: str = ""


def get_wait_for_callback_context() -> WaitForCallbackContext:
    """Return the active `WaitForCallbackContext`."""
    current_context = get_current_context()
    if not isinstance(current_context, WaitForCallbackContext):
        msg = (
            "get_wait_for_callback_context() can only be used while a "
            "wait_for_callback submitter is executing."
        )
        raise RuntimeError(msg)
    return current_context

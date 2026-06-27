"""Composite wait_for_callback operation built from callback, step, and child context."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..config import Duration
from ..context import get_current_context
from ..context import invoke_user_callable
from ..execution import durable_callable
from ..models import RetryDecision
from ..primitive.base import OperationContext
from ..primitive.callback import Callback, create_callback
from ..primitive.child import run_in_child_context
from ..primitive.step import step

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ..serdes import SerDes

logger = logging.getLogger(__name__)


@durable_callable
async def wait_for_callback_handler(
    submitter: Callable[[], Awaitable[Any]],
    name: str | None = None,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes | None = None,
    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None,
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

    async def submitter_step():
        step_context = get_current_context()
        callback_context = WaitForCallbackContext(
            callback_id=callback.callback_id,
            execution_state=step_context.execution_state,
            operation_identifier=step_context.operation_identifier,
        )
        return await invoke_user_callable(callback_context, submitter)

    await step(
        func=submitter_step,
        name=submitter_step_name,
        retry_strategy=retry_strategy,
        serdes=serdes,
    )

    return await callback.result()


async def wait_for_callback(
    submitter: Callable[[], Awaitable[Any]],
    *,
    name: str | None = None,
    timeout: Duration | None = None,
    heartbeat_timeout: Duration | None = None,
    serdes: SerDes | None = None,
    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None,
) -> Any:
    """Create a callback, run a submitter, then suspend until the callback resolves.

    Args:
        submitter: Async callable. Use get_current_context().callback_id inside the
            submitter to access the callback id.
        name: Optional durable operation name.
        timeout: Optional maximum time to wait for callback completion.
        heartbeat_timeout: Optional maximum time to wait between callback heartbeats.
        serdes: Optional serializer for callback results and submitter results.
        retry_strategy: Optional retry strategy for submitter failures.
    """
    context_name = name if name is not None else getattr(submitter, "__name__", None)
    logger.debug("wait_for_callback name: %s", context_name)

    return await run_in_child_context(
        wait_for_callback_handler(
            submitter,
            name=context_name,
            timeout=timeout,
            heartbeat_timeout=heartbeat_timeout,
            serdes=serdes,
            retry_strategy=retry_strategy,
        ),
        name=context_name,
    )


@dataclass(frozen=True)
class WaitForCallbackContext(OperationContext):
    """Context available during wait_for_callback submitter execution."""

    callback_id: str = ""

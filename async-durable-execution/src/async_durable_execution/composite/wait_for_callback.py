"""Composite wait_for_callback operation built from callback, step, and child context."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from ..async_tools import assert_async_callable, get_callable_name
from ..context import get_current_context, reset_current_context, set_current_context
from ..models import RetryDecision
from ..primitive.base import OperationContext
from ..primitive.callback import Callback, CallbackConfig, create_callback
from ..primitive.child import (
    _get_durable_context,
    _run_in_child_context_in_context,
)
from ..primitive.step import step as step_operation

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ..primitive.child import DurableContext
    from ..serdes import SerDes

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WaitForCallbackConfig(CallbackConfig):
    """Configuration for wait for callback."""

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None


async def wait_for_callback_handler(
    context: DurableContext,
    submitter: Callable[[], Awaitable[Any]],
    name: str | None = None,
    config: WaitForCallbackConfig | None = None,
) -> Any:
    """Create a callback, run a submitter, and wait for callback completion."""
    name_with_space: str = f"{name} " if name else ""
    callback: Callback = await create_callback(
        name=f"{name_with_space}create callback id",
        timeout=config.timeout if config else None,
        heartbeat_timeout=config.heartbeat_timeout if config else None,
        serdes=config.serdes if config else None,
    )

    async def submitter_step():
        step_context = get_current_context()
        callback_context = WaitForCallbackContext(
            callback_id=callback.callback_id,
            execution_state=step_context.execution_state,
            operation_identifier=step_context.operation_identifier,
            lambda_context=step_context.lambda_context,
        )
        token = set_current_context(callback_context)
        try:
            return await submitter()
        finally:
            reset_current_context(token)

    await step_operation(
        func=submitter_step,
        name=f"{name_with_space}submitter",
        retry_strategy=config.retry_strategy if config else None,
        serdes=config.serdes if config else None,
    )

    return await callback.result()


async def wait_for_callback(
    submitter: Callable[[], Awaitable[Any]],
    *,
    name: str | None = None,
    timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
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
    context = _get_durable_context("wait_for_callback")
    assert_async_callable(submitter, label="submitter")
    step_name: str | None = name or get_callable_name(submitter)
    logger.debug("wait_for_callback name: %s", step_name)
    config = WaitForCallbackConfig(
        timeout=timeout if timeout is not None else timedelta(),
        heartbeat_timeout=heartbeat_timeout
        if heartbeat_timeout is not None
        else timedelta(),
        serdes=serdes,
        retry_strategy=retry_strategy,
    )

    async def wait_in_child_context():
        current_context = get_current_context()
        return await wait_for_callback_handler(
            current_context,
            submitter,
            step_name,
            config,
        )

    return await _run_in_child_context_in_context(
        context,
        wait_in_child_context,
        name=step_name,
    )


@dataclass(frozen=True)
class WaitForCallbackContext(OperationContext):
    """Context available during wait_for_callback submitter execution."""

    callback_id: str = ""

"""Unit tests for the with_retry helper function."""

from __future__ import annotations
from typing import no_type_check
from typing import Any

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TypeVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from async_durable_execution import (
    WithRetryContext,
    get_with_retry_context,
    with_retry,
    with_retry as imported_with_retry,
)
from async_durable_execution._core.config import (
    Duration,
    JitterStrategy,
    RetryStrategy,
)
from async_durable_execution._core.context import (
    DurableContext,
    bind_current_context,
    get_current_context,
    reset_current_context,
    set_current_context,
)
from async_durable_execution._core.exceptions import SuspendExecution
from async_durable_execution._core.models import OperationIdentifier, OperationSubType


_T = TypeVar("_T")


def _make_durable_context() -> DurableContext:
    state = MagicMock()
    state.durable_execution_arn = "arn:aws:test"
    state.lambda_context = None
    return DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id="parent",
        ),
    )


def _current_attempt() -> int:
    return get_with_retry_context().attempt


def test_get_with_retry_context_returns_bound_retry_context() -> None:
    durable_context = _make_durable_context()
    retry_context = WithRetryContext(
        execution_state=durable_context.execution_state,
        operation_identifier=durable_context.operation_identifier,
        attempt=3,
    )

    with bind_current_context(retry_context):
        context = get_with_retry_context()

    assert context is retry_context
    assert context.attempt == 3


def test_get_with_retry_context_rejects_non_retry_context() -> None:
    with (
        bind_current_context(_make_durable_context()),
        pytest.raises(
            RuntimeError,
            match=r"get_with_retry_context\(\) can only be used while a with_retry body is executing\.",
        ),
    ):
        get_with_retry_context()


@dataclass
class WaitCall:
    """Record of a wait() call."""

    duration: Duration
    name: str | None


@dataclass
class RunInChildContextCall:
    """Record of a run_in_child_context() call."""

    name: str | None
    serdes: object = None
    summary_generator: object = None
    is_virtual: bool = False
    result: object = None


@dataclass
class MockDurableContext:
    """A fake DurableContext that records wait() and run_in_child_context() calls."""

    wait_calls: list[WaitCall] = field(default_factory=list)
    child_context_calls: list[RunInChildContextCall] = field(default_factory=list)

    async def wait(self, duration: Duration, name: str | None = None) -> None:
        self.wait_calls.append(WaitCall(duration=duration, name=name))

    async def run_in_child_context(
        self,
        func: Callable[[MockDurableContext], Awaitable[_T]],
        name: str | None = None,
        serdes=None,
        summary_generator=None,
        is_virtual: bool = False,
    ) -> _T:
        result: _T = await func(self)
        self.child_context_calls.append(
            RunInChildContextCall(
                name=name,
                serdes=serdes,
                summary_generator=summary_generator,
                is_virtual=is_virtual,
                result=result,
            )
        )
        return result

    def step(self, *args, **kwargs) -> Any:
        raise NotImplementedError("step not used in with_retry tests")

    def map(self, *args, **kwargs) -> Any:
        raise NotImplementedError("map not used in with_retry tests")

    def parallel(self, *args, **kwargs) -> Any:
        raise NotImplementedError("parallel not used in with_retry tests")

    def create_callback(self, *args, **kwargs) -> Any:
        raise NotImplementedError("create_callback not used in with_retry tests")


async def _call_with_retry(
    ctx: MockDurableContext,
    func,
    *,
    name: str | None = None,
    retry_strategy=None,
    serdes=None,
    summary_generator=None,
    is_virtual: bool = False,
) -> Any:
    """Invoke with_retry() against a patched ambient context."""

    async def fake_wait(
        duration: Duration,
        *,
        name: str | None = None,
    ) -> None:
        ctx.wait_calls.append(WaitCall(duration=duration, name=name))

    async def fake_run_in_child_context(
        func,
        name: str | None = None,
        serdes=None,
        summary_generator=None,
        is_virtual: bool = False,
    ) -> Any:
        token = set_current_context(_make_durable_context())
        try:
            result = await func()
        finally:
            reset_current_context(token)
        ctx.child_context_calls.append(
            RunInChildContextCall(
                name=name,
                serdes=serdes,
                summary_generator=summary_generator,
                is_virtual=is_virtual,
                result=result,
            )
        )
        return result

    with (
        patch(
            "async_durable_execution._extension.with_retry.wait",
            new=AsyncMock(side_effect=fake_wait),
        ),
        patch(
            "async_durable_execution._extension.with_retry.run_in_child_context",
            new=AsyncMock(side_effect=fake_run_in_child_context),
        ),
    ):
        return await with_retry(
            func,
            name=name,
            retry_strategy=retry_strategy,
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
        )


def _make_retry_strategy(
    max_attempts: int = 3,
    initial_delay: timedelta | None = None,
) -> Any:
    """Create a retry strategy with no jitter for deterministic tests."""
    return RetryStrategy(
        max_attempts=max_attempts,
        initial_delay=initial_delay or timedelta(seconds=1),
        jitter_strategy=JitterStrategy.NONE,
    )


async def test_success_on_first_attempt_returns_result_without_retry() -> None:
    """Function succeeds on first attempt returns result without invoking retry strategy."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy()

    async def tracking_func() -> str:
        return "success"

    result = await _call_with_retry(
        ctx,
        tracking_func,
        retry_strategy=retry_strategy,
    )

    assert result == "success"
    assert len(ctx.wait_calls) == 0


async def test_function_fails_then_succeeds_returns_successful_result() -> None:
    """Function fails then succeeds returns result from successful attempt."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=3)

    call_count = 0

    async def failing_then_succeeding() -> str:
        nonlocal call_count
        attempt = _current_attempt()
        call_count += 1
        if attempt < 3:
            raise ValueError(f"fail on attempt {attempt}")
        return "eventual success"

    result = await _call_with_retry(
        ctx,
        failing_then_succeeding,
        retry_strategy=retry_strategy,
    )

    assert result == "eventual success"
    assert call_count == 3
    assert len(ctx.wait_calls) == 2


async def test_async_function_fails_then_succeeds_returns_successful_result() -> None:
    """Async retry body is awaited inside the retry loop."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=3)

    call_count = 0

    async def failing_then_succeeding() -> str:
        nonlocal call_count
        await asyncio.sleep(0)
        attempt = _current_attempt()
        call_count += 1
        if attempt < 3:
            raise ValueError(f"fail on attempt {attempt}")
        return "eventual success"

    result = await _call_with_retry(
        ctx,
        failing_then_succeeding,
        retry_strategy=retry_strategy,
    )

    assert result == "eventual success"
    assert call_count == 3
    assert len(ctx.wait_calls) == 2


async def test_retry_strategy_returns_none_to_stop_retries() -> None:
    """Retry strategy returns None to stop retrying and surface the exception."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=1)

    async def always_fails() -> None:
        raise RuntimeError("permanent failure")

    with pytest.raises(RuntimeError, match="permanent failure"):
        await _call_with_retry(
            ctx,
            always_fails,
            retry_strategy=retry_strategy,
        )

    assert len(ctx.wait_calls) == 0


async def test_suspend_execution_is_reraised_immediately() -> None:
    """SuspendExecution is re-raised immediately without invoking retry strategy."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=5)

    async def raises_suspend() -> None:
        raise SuspendExecution("suspending")

    with pytest.raises(SuspendExecution, match="suspending"):
        await _call_with_retry(
            ctx,
            raises_suspend,
            retry_strategy=retry_strategy,
        )

    assert len(ctx.wait_calls) == 0


async def test_async_suspend_execution_is_reraised_immediately() -> None:
    """SuspendExecution raised after an await still bypasses retry handling."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=5)

    async def raises_suspend() -> None:
        await asyncio.sleep(0)
        raise SuspendExecution("suspending")

    with pytest.raises(SuspendExecution, match="suspending"):
        await _call_with_retry(
            ctx,
            raises_suspend,
            retry_strategy=retry_strategy,
        )

    assert len(ctx.wait_calls) == 0


async def test_default_config_wraps_in_child_context() -> None:
    """Default config runs the retry loop inside run_in_child_context."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy()

    async def simple_func() -> str:
        return "ok"

    result = await _call_with_retry(ctx, simple_func, retry_strategy=retry_strategy)

    assert result == "ok"
    assert len(ctx.child_context_calls) == 1


async def test_retry_body_runs_with_child_context_bound() -> None:
    """The retry body keeps the child context installed by run_in_child_context."""
    child_ctx = _make_durable_context()

    async def fake_run_in_child_context(func, **_kwargs) -> Any:
        token = set_current_context(child_ctx)
        try:
            return await func()
        finally:
            reset_current_context(token)

    async def current_context_is_child() -> bool:
        context = get_current_context()
        return (
            isinstance(context, WithRetryContext)
            and context.attempt == 1
            and context.execution_state is child_ctx.execution_state
            and context.operation_identifier is child_ctx.operation_identifier
        )

    with (
        patch(
            "async_durable_execution._extension.with_retry.run_in_child_context",
            new=AsyncMock(side_effect=fake_run_in_child_context),
        ),
        patch(
            "async_durable_execution._extension.with_retry.get_durable_context",
            return_value=child_ctx,
        ),
    ):
        result = await with_retry(current_context_is_child)

    assert result is True


@no_type_check
async def test_default_retry_strategy_is_used_when_not_provided() -> None:
    """Default retry strategy retries transient failures."""
    ctx = MockDurableContext()
    call_count = 0

    async def fails_once() -> str:
        nonlocal call_count
        attempt = _current_attempt()
        call_count += 1
        if attempt == 1:
            raise ValueError("transient")
        return "ok"

    result = await _call_with_retry(ctx, fails_once)

    assert result == "ok"
    assert call_count == 2
    assert len(ctx.wait_calls) == 1
    assert ctx.wait_calls[0].duration > 0
    assert ctx.wait_calls[0].name == "backoff-1"


async def test_no_name_creates_default_child_context_and_backoff_waits() -> None:
    """Missing name uses stable default child and wait operation names."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=3)

    call_count = 0

    async def fails_once() -> str:
        nonlocal call_count
        attempt = _current_attempt()
        call_count += 1
        if attempt == 1:
            raise ValueError("first failure")
        return "ok"

    result = await _call_with_retry(
        ctx,
        fails_once,
        name=None,
        retry_strategy=retry_strategy,
    )

    assert result == "ok"
    assert ctx.child_context_calls == [
        RunInChildContextCall(name="with-retry", result="ok")
    ]
    assert ctx.wait_calls == [WaitCall(duration=1, name="backoff-1")]


async def test_name_is_forwarded_to_child_context_and_backoff_waits() -> None:
    """Provided name is reused for child context and derived wait names."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=3)

    async def fails_twice() -> str:
        attempt = _current_attempt()
        if attempt < 3:
            raise RuntimeError("transient")
        return "done"

    result = await _call_with_retry(
        ctx,
        fails_twice,
        name="my-retry",
        retry_strategy=retry_strategy,
    )

    assert result == "done"
    assert ctx.child_context_calls == [
        RunInChildContextCall(name="my-retry", result="done")
    ]
    assert ctx.wait_calls == [
        WaitCall(duration=1, name="my-retry-backoff-1"),
        WaitCall(duration=2, name="my-retry-backoff-2"),
    ]


async def test_child_context_fields_are_forwarded() -> None:
    """Child context fields are forwarded to run_in_child_context."""
    ctx = MockDurableContext()
    serdes = MagicMock()
    summary_generator = MagicMock()
    retry_strategy = _make_retry_strategy(max_attempts=2)

    async def simple_func() -> str:
        return "ok"

    await _call_with_retry(
        ctx,
        simple_func,
        name="test",
        retry_strategy=retry_strategy,
        serdes=serdes,
        summary_generator=summary_generator,
        is_virtual=True,
    )

    assert ctx.child_context_calls == [
        RunInChildContextCall(
            name="test",
            serdes=serdes,
            summary_generator=summary_generator,
            is_virtual=True,
            result="ok",
        )
    ]


async def test_attempt_number_starts_at_1_and_increments() -> None:
    """Attempt numbers start at one and increment across retries."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=5)
    attempts_seen: list[int] = []

    async def record_attempts() -> str:
        attempt = _current_attempt()
        attempts_seen.append(attempt)
        if attempt < 4:
            raise ValueError("keep retrying")
        return "ok"

    result = await _call_with_retry(
        ctx,
        record_attempts,
        retry_strategy=retry_strategy,
    )

    assert result == "ok"
    assert attempts_seen == [1, 2, 3, 4]


async def test_with_retry_importable_from_package() -> None:
    """with_retry is re-exported from the package root."""
    assert callable(imported_with_retry)
    assert callable(with_retry)


@no_type_check
async def test_with_retry_strategy_is_not_positional_parameter() -> None:
    """with_retry rejects retry strategies as positional arguments."""

    async def test_function() -> str:
        attempt = _current_attempt()
        return f"attempt-{attempt}"

    with pytest.raises(TypeError):
        await with_retry(
            test_function,
            lambda _err, _attempt: 1,
        )


async def test_integration_with_retry_strategy() -> None:
    """Integration test with the real RetryStrategy."""
    ctx = MockDurableContext()

    retry_strategy = RetryStrategy(
        max_attempts=4,
        initial_delay=timedelta(seconds=2),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.NONE,
    )

    async def fails_three_times() -> str:
        attempt = _current_attempt()
        if attempt < 4:
            raise ValueError("transient")
        return "done"

    result = await _call_with_retry(
        ctx,
        fails_three_times,
        retry_strategy=retry_strategy,
    )

    assert result == "done"
    assert ctx.wait_calls == [
        WaitCall(duration=2, name="backoff-1"),
        WaitCall(duration=4, name="backoff-2"),
        WaitCall(duration=8, name="backoff-3"),
    ]


async def test_integration_retries_exhausted_raises_last_exception() -> None:
    """Exhausting retries surfaces the final exception."""
    ctx = MockDurableContext()

    retry_strategy = RetryStrategy(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        jitter_strategy=JitterStrategy.NONE,
    )

    async def always_fails() -> None:
        attempt = _current_attempt()
        raise RuntimeError(f"failure-{attempt}")

    with pytest.raises(RuntimeError, match="failure-3"):
        await _call_with_retry(
            ctx,
            always_fails,
            retry_strategy=retry_strategy,
        )

    assert ctx.wait_calls == [
        WaitCall(duration=1, name="backoff-1"),
        WaitCall(duration=2, name="backoff-2"),
    ]

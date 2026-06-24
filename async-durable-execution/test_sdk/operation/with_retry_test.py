"""Unit tests for the with_retry helper function."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, TypeVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from async_durable_execution import with_retry, with_retry as imported_with_retry
from async_durable_execution.config import (
    JitterStrategy,
    RetryStrategyBuilder,
)
from async_durable_execution.exceptions import SuspendExecution
from async_durable_execution.operation.child import ChildConfig
from async_durable_execution.operation.with_retry import WithRetryConfig


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from async_durable_execution import DurableContext

_T = TypeVar("_T")


@dataclass
class WaitCall:
    """Record of a wait() call."""

    duration: timedelta
    name: str | None


@dataclass
class RunInChildContextCall:
    """Record of a run_in_child_context() call."""

    name: str | None
    config: ChildConfig | None
    result: object = None


@dataclass
class MockDurableContext:
    """A fake DurableContext that records wait() and run_in_child_context() calls."""

    wait_calls: list[WaitCall] = field(default_factory=list)
    child_context_calls: list[RunInChildContextCall] = field(default_factory=list)

    async def wait(self, duration: timedelta, name: str | None = None) -> None:
        self.wait_calls.append(WaitCall(duration=duration, name=name))

    async def run_in_child_context(
        self,
        func: Callable[[DurableContext], Awaitable[_T]],
        name: str | None = None,
        config: ChildConfig | None = None,
    ) -> _T:
        result: _T = await func(self)  # type: ignore[arg-type]
        self.child_context_calls.append(
            RunInChildContextCall(name=name, config=config, result=result)
        )
        return result

    def step(self, *args, **kwargs):
        raise NotImplementedError("step not used in with_retry tests")

    def map(self, *args, **kwargs):
        raise NotImplementedError("map not used in with_retry tests")

    def parallel(self, *args, **kwargs):
        raise NotImplementedError("parallel not used in with_retry tests")

    def create_callback(self, *args, **kwargs):
        raise NotImplementedError("create_callback not used in with_retry tests")


def test_with_retry_config_defaults():
    """WithRetryConfig defaults align with the public API."""
    config = WithRetryConfig()

    assert config.retry_strategy is None
    assert config.serdes is None
    assert config.item_serdes is None
    assert config.summary_generator is None
    assert config.is_virtual is False


def test_with_retry_config_custom_values():
    """WithRetryConfig stores explicit values."""
    retry_strategy = MagicMock()
    serdes = MagicMock()
    item_serdes = MagicMock()
    summary_generator = MagicMock()

    config = WithRetryConfig(
        retry_strategy=retry_strategy,
        serdes=serdes,
        item_serdes=item_serdes,
        summary_generator=summary_generator,
        is_virtual=True,
    )

    assert config.retry_strategy is retry_strategy
    assert config.serdes is serdes
    assert config.item_serdes is item_serdes
    assert config.summary_generator is summary_generator
    assert config.is_virtual is True


async def _call_with_retry(
    ctx: MockDurableContext,
    func,
    *,
    name: str | None = None,
    retry_strategy=None,
    serdes=None,
    item_serdes=None,
    summary_generator=None,
    is_virtual: bool = False,
):
    """Invoke with_retry() against a patched ambient context."""

    async def fake_wait_in_context(
        context: MockDurableContext,
        duration: timedelta,
        name: str | None = None,
    ) -> None:
        assert context is ctx
        ctx.wait_calls.append(WaitCall(duration=duration, name=name))

    async def fake_run_in_child_context_in_context(
        context: MockDurableContext,
        func,
        name: str | None = None,
        config=None,
    ):
        assert context is ctx
        result = await func()
        ctx.child_context_calls.append(
            RunInChildContextCall(name=name, config=config, result=result)
        )
        return result

    with (
        patch(
            "async_durable_execution.operation.with_retry._get_durable_context",
            return_value=ctx,
        ),
        patch(
            "async_durable_execution.operation.with_retry._wait_in_context",
            new=AsyncMock(side_effect=fake_wait_in_context),
        ),
        patch(
            "async_durable_execution.operation.with_retry._run_in_child_context_in_context",
            new=AsyncMock(side_effect=fake_run_in_child_context_in_context),
        ),
    ):
        return await with_retry(
            func,
            name=name,
            retry_strategy=retry_strategy,
            serdes=serdes,
            item_serdes=item_serdes,
            summary_generator=summary_generator,
            is_virtual=is_virtual,
        )


def _make_retry_strategy(
    max_attempts: int = 3,
    initial_delay: timedelta | None = None,
):
    """Create a retry strategy with no jitter for deterministic tests."""
    return RetryStrategyBuilder(
        max_attempts=max_attempts,
        initial_delay=initial_delay or timedelta(seconds=1),
        jitter_strategy=JitterStrategy.NONE,
    ).build()


async def test_success_on_first_attempt_returns_result_without_retry():
    """Function succeeds on first attempt returns result without invoking retry strategy."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy()

    async def tracking_func(attempt: int) -> str:
        return "success"

    result = await _call_with_retry(
        ctx,
        tracking_func,
        retry_strategy=retry_strategy,
    )

    assert result == "success"
    assert len(ctx.wait_calls) == 0


async def test_function_fails_then_succeeds_returns_successful_result():
    """Function fails then succeeds returns result from successful attempt."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=3)

    call_count = 0

    async def failing_then_succeeding(attempt: int) -> str:
        nonlocal call_count
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


async def test_async_function_fails_then_succeeds_returns_successful_result():
    """Async retry body is awaited inside the retry loop."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=3)

    call_count = 0

    async def failing_then_succeeding(attempt: int) -> str:
        nonlocal call_count
        await asyncio.sleep(0)
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


async def test_retry_strategy_returns_should_retry_false_reraises_exception():
    """Retry strategy returns should_retry=False re-raises exception."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=1)

    async def always_fails(attempt: int) -> None:
        raise RuntimeError("permanent failure")

    with pytest.raises(RuntimeError, match="permanent failure"):
        await _call_with_retry(
            ctx,
            always_fails,
            retry_strategy=retry_strategy,
        )

    assert len(ctx.wait_calls) == 0


async def test_suspend_execution_is_reraised_immediately():
    """SuspendExecution is re-raised immediately without invoking retry strategy."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=5)

    async def raises_suspend(attempt: int) -> None:
        raise SuspendExecution("suspending")

    with pytest.raises(SuspendExecution, match="suspending"):
        await _call_with_retry(
            ctx,
            raises_suspend,
            retry_strategy=retry_strategy,
        )

    assert len(ctx.wait_calls) == 0


async def test_async_suspend_execution_is_reraised_immediately():
    """SuspendExecution raised after an await still bypasses retry handling."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=5)

    async def raises_suspend(attempt: int) -> None:
        await asyncio.sleep(0)
        raise SuspendExecution("suspending")

    with pytest.raises(SuspendExecution, match="suspending"):
        await _call_with_retry(
            ctx,
            raises_suspend,
            retry_strategy=retry_strategy,
        )

    assert len(ctx.wait_calls) == 0


async def test_default_config_wraps_in_child_context():
    """Default config runs the retry loop inside run_in_child_context."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy()

    async def simple_func(attempt: int) -> str:
        return "ok"

    result = await _call_with_retry(ctx, simple_func, retry_strategy=retry_strategy)

    assert result == "ok"
    assert len(ctx.child_context_calls) == 1


async def test_no_name_creates_anonymous_child_context_and_anonymous_waits():
    """Missing name preserves anonymous child/wait operations."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=3)

    call_count = 0

    async def fails_once(attempt: int) -> str:
        nonlocal call_count
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
        RunInChildContextCall(name=None, config=ChildConfig(), result="ok")
    ]
    assert ctx.wait_calls == [WaitCall(duration=timedelta(seconds=1), name=None)]


async def test_name_is_forwarded_to_child_context_and_backoff_waits():
    """Provided name is reused for child context and derived wait names."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=3)

    async def fails_twice(attempt: int) -> str:
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
        RunInChildContextCall(name="my-retry", config=ChildConfig(), result="done")
    ]
    assert ctx.wait_calls == [
        WaitCall(duration=timedelta(seconds=1), name="my-retry-backoff-1"),
        WaitCall(duration=timedelta(seconds=2), name="my-retry-backoff-2"),
    ]


async def test_child_context_fields_are_forwarded():
    """Child context fields are forwarded to run_in_child_context."""
    ctx = MockDurableContext()
    serdes = MagicMock()
    item_serdes = MagicMock()
    summary_generator = MagicMock()
    retry_strategy = _make_retry_strategy(max_attempts=2)

    async def simple_func(attempt: int) -> str:
        return "ok"

    await _call_with_retry(
        ctx,
        simple_func,
        name="test",
        retry_strategy=retry_strategy,
        serdes=serdes,
        item_serdes=item_serdes,
        summary_generator=summary_generator,
        is_virtual=True,
    )

    assert ctx.child_context_calls == [
        RunInChildContextCall(
            name="test",
            config=ChildConfig(
                serdes=serdes,
                item_serdes=item_serdes,
                summary_generator=summary_generator,
                is_virtual=True,
            ),
            result="ok",
        )
    ]


async def test_attempt_number_starts_at_1_and_increments():
    """Attempt numbers start at one and increment across retries."""
    ctx = MockDurableContext()
    retry_strategy = _make_retry_strategy(max_attempts=5)
    attempts_seen: list[int] = []

    async def record_attempts(attempt: int) -> str:
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


async def test_with_retry_importable_from_package():
    """with_retry is re-exported from the package root."""
    assert callable(imported_with_retry)
    assert callable(with_retry)


async def test_with_retry_config_is_not_positional_parameter():
    """with_retry rejects the removed positional config argument."""

    async def test_function(attempt: int) -> str:
        return f"attempt-{attempt}"

    with pytest.raises(TypeError):
        await with_retry(test_function, WithRetryConfig())


async def test_integration_with_retry_strategy_builder():
    """Integration test with the real RetryStrategyBuilder."""
    ctx = MockDurableContext()

    retry_strategy = RetryStrategyBuilder(
        max_attempts=4,
        initial_delay=timedelta(seconds=2),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.NONE,
    ).build()

    async def fails_three_times(attempt: int) -> str:
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
        WaitCall(duration=timedelta(seconds=2), name=None),
        WaitCall(duration=timedelta(seconds=4), name=None),
        WaitCall(duration=timedelta(seconds=8), name=None),
    ]


async def test_integration_retries_exhausted_raises_last_exception():
    """Exhausting retries surfaces the final exception."""
    ctx = MockDurableContext()

    retry_strategy = RetryStrategyBuilder(
        max_attempts=3,
        initial_delay=timedelta(seconds=1),
        jitter_strategy=JitterStrategy.NONE,
    ).build()

    async def always_fails(attempt: int) -> None:
        raise RuntimeError(f"failure-{attempt}")

    with pytest.raises(RuntimeError, match="failure-3"):
        await _call_with_retry(
            ctx,
            always_fails,
            retry_strategy=retry_strategy,
        )

    assert ctx.wait_calls == [
        WaitCall(duration=timedelta(seconds=1), name=None),
        WaitCall(duration=timedelta(seconds=2), name=None),
    ]

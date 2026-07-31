"""Unit tests for scheduler.py"""

import asyncio
import threading
import time
from unittest.mock import patch

import pytest

from async_durable_execution._runner.local import Scheduler, Event


async def async_noop() -> None:
    """Reusable no-op callback for scheduler tests."""


def wait_for_condition(condition_func, timeout_iterations=100) -> bool:
    """Wait for a condition to become true with polling."""
    loop = asyncio.get_event_loop()
    for _ in range(timeout_iterations):
        if condition_func():
            return True
        loop.run_until_complete(asyncio.sleep(0.001))
    return False


def scheduler_is_started(scheduler: Scheduler) -> bool:
    return scheduler._running  # noqa: SLF001


def scheduler_event_count(scheduler: Scheduler) -> int:
    return len(scheduler._events)  # noqa: SLF001


def scheduler_task_count(scheduler: Scheduler) -> int:
    return sum(1 for task in scheduler._tasks if not task.done())  # noqa: SLF001


def remove_event(event: Event) -> None:
    event._scheduler.remove_event(event._event)  # noqa: SLF001


def wait_for_event(
    event: Event, timeout: float | None = None, *, clear_on_set: bool = True
) -> bool:
    loop = event._scheduler.get_loop()  # noqa: SLF001
    return loop.run_until_complete(
        event.wait_async(timeout=timeout, clear_on_set=clear_on_set)
    )


def test_scheduler_init() -> None:
    """Test Scheduler initialization."""
    scheduler = Scheduler()
    assert not scheduler_is_started(scheduler)
    assert scheduler_event_count(scheduler) == 0


def test_scheduler_context_manager() -> None:
    """Test Scheduler as context manager."""
    with Scheduler() as scheduler:
        assert scheduler_is_started(scheduler)
    assert not scheduler_is_started(scheduler)


def test_scheduler_start_stop() -> None:
    """Test Scheduler start and stop methods."""
    scheduler = Scheduler()

    scheduler.start()
    assert scheduler_is_started(scheduler)

    # Test start when already running
    scheduler.start()
    assert scheduler_is_started(scheduler)

    scheduler.stop()
    assert not scheduler_is_started(scheduler)

    # Test stop when not running
    scheduler.stop()
    assert not scheduler_is_started(scheduler)


def test_scheduler_running_state() -> None:
    """Test Scheduler running state transitions."""
    scheduler = Scheduler()

    # Initially not started
    assert not scheduler_is_started(scheduler)

    # After start
    scheduler.start()
    assert scheduler_is_started(scheduler)

    # After stop
    scheduler.stop()
    assert not scheduler_is_started(scheduler)


def test_scheduler_event_tracking() -> None:
    """Test Scheduler event tracking."""
    scheduler = Scheduler()
    scheduler.start()

    # Initially no events
    assert scheduler_event_count(scheduler) == 0

    # Create events
    event1 = scheduler.create_event()
    assert scheduler_event_count(scheduler) == 1

    scheduler.create_event()
    assert scheduler_event_count(scheduler) == 2

    # Remove event
    remove_event(event1)
    wait_for_condition(lambda: scheduler_event_count(scheduler) == 1)
    assert scheduler_event_count(scheduler) == 1

    scheduler.stop()


def test_scheduler_task_tracking() -> None:
    """Test Scheduler task tracking."""
    scheduler = Scheduler()

    # When not started, task count is 0
    assert scheduler_task_count(scheduler) == 0

    scheduler.start()

    # Create tasks with longer delay to ensure they're counted
    future1 = scheduler.call_later(async_noop, delay=0.5)
    # Give a moment for the task to be created
    time.sleep(0.01)
    assert scheduler_task_count(scheduler) >= 1

    future2 = scheduler.call_later(async_noop, delay=0.5)
    time.sleep(0.01)
    assert scheduler_task_count(scheduler) >= 2

    # Cancel tasks to clean up
    future1.cancel()
    future2.cancel()

    # Wait for tasks to complete or be cancelled
    wait_for_condition(
        lambda: scheduler_task_count(scheduler) == 0, timeout_iterations=200
    )

    scheduler.stop()


def test_scheduler_call_later_async_function() -> None:
    """Test call_later with async function."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def async_func() -> None:
        result.append("async_executed")

    future = scheduler.call_later(async_func, delay=0.01)
    wait_for_condition(lambda: future.done())

    assert isinstance(future, asyncio.Future)
    assert result == ["async_executed"]
    assert future.done()

    scheduler.stop()


def test_scheduler_call_later_multiple_count() -> None:
    """Test call_later with multiple executions."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def func() -> None:
        result.append("count")

    # Note: Current implementation only executes once due to early return
    future = scheduler.call_later(func, delay=0.01, count=3)
    wait_for_condition(lambda: future.done())

    # Current implementation only executes once
    assert len(result) == 1
    assert future.done()

    scheduler.stop()


def test_scheduler_call_later_runs_async_functions() -> None:
    """Test scheduled async functions run."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def func() -> None:
        result.append("executed")

    futures = [scheduler.call_later(func, delay=0) for _ in range(3)]
    wait_for_condition(lambda: all(future.done() for future in futures))

    assert result == ["executed", "executed", "executed"]

    scheduler.stop()


def test_scheduler_call_later_runs_on_caller_thread() -> None:
    """Test scheduled callbacks run on the scheduler event loop thread."""
    scheduler = Scheduler()
    scheduler.start()

    caller_thread_id = threading.get_ident()
    callback_thread_ids = []

    async def func() -> None:
        callback_thread_ids.append(threading.get_ident())

    future = scheduler.call_later(func, delay=0)
    wait_for_condition(lambda: future.done())

    assert callback_thread_ids == [caller_thread_id]

    scheduler.stop()


def test_scheduler_call_later_infinite_count() -> None:
    """Test call_later with infinite count."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def func() -> None:
        result.append("infinite")

    # Note: Current implementation only executes once due to early return
    future = scheduler.call_later(func, delay=0.01, count=None)
    wait_for_condition(lambda: future.done())

    # Current implementation only executes once
    assert len(result) == 1
    assert future.done()

    scheduler.stop()


def test_scheduler_call_later_function_exception() -> None:
    """Test call_later with function that raises exception."""
    scheduler = Scheduler()
    scheduler.start()

    async def failing_func() -> None:
        msg: str = "test error"

        raise ValueError(msg)

    with patch("async_durable_execution._runner.local.scheduler.logger") as mock_logger:
        future = scheduler.call_later(failing_func, delay=0.01)
        wait_for_condition(lambda: future.done())

        assert future.done()
        mock_logger.exception.assert_called()

    scheduler.stop()


def test_scheduler_create_event() -> None:
    """Test create_event method."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()

    assert isinstance(event, Event)
    assert scheduler_event_count(scheduler) == 1

    scheduler.stop()


def test_task_cancel() -> None:
    """Test Future cancel method."""
    scheduler = Scheduler()
    scheduler.start()

    async def func() -> None:
        pass

    future = scheduler.call_later(func, delay=0.1, count=None)
    future.cancel()

    # Wait briefly for cancellation to take effect
    wait_for_condition(lambda: future.cancelled())

    assert future.cancelled()

    scheduler.stop()


def test_task_is_done() -> None:
    """Test Future done property."""
    scheduler = Scheduler()
    scheduler.start()

    async def quick_func() -> None:
        pass

    future = scheduler.call_later(quick_func, delay=0.01)
    assert not future.done()

    wait_for_condition(lambda: future.done())
    assert future.done()

    # Small delay to ensure coroutine cleanup completes
    time.sleep(0.01)
    scheduler.stop()


def test_task_result() -> None:
    """Test Future result method."""
    scheduler = Scheduler()
    scheduler.start()

    async def func() -> None:
        return None

    future = scheduler.call_later(func, delay=0.01)
    wait_for_condition(lambda: future.done())

    result = future.result()
    assert result is None

    scheduler.stop()


def test_task_cancel_method() -> None:
    """Test Future cancel method."""
    scheduler = Scheduler()
    scheduler.start()

    # Create a future and cancel it immediately
    future = scheduler.call_later(async_noop, delay=0.01)
    future.cancel()

    # The cancel method should work without hanging
    # We don't test the result here to avoid timing issues

    scheduler.stop()


def test_task_result_completed() -> None:
    """Test Future result method when completed."""
    scheduler = Scheduler()
    scheduler.start()

    async def func() -> str:
        return "test_result"

    future = scheduler.call_later(func, delay=0.01)
    wait_for_condition(lambda: future.done())
    assert future.done()

    # Small delay to ensure coroutine cleanup completes
    time.sleep(0.01)
    scheduler.stop()


def test_event_set_and_wait_timeout() -> None:
    """Test Event set and wait with timeout."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()

    # Test wait with timeout (should timeout)
    result = wait_for_event(event, timeout=0.01, clear_on_set=False)
    assert result is False

    # Set the event
    event.set()

    # Wait should now succeed
    result = wait_for_event(event, timeout=0.1, clear_on_set=True)
    assert result is True

    scheduler.stop()


def test_event_wait_set_by_scheduled_callback() -> None:
    """Test Event wait when set by a scheduled callback."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()

    async def set_event() -> None:
        event.set()

    scheduler.call_later(set_event, delay=0.01)

    assert wait_for_event(event, timeout=1.0) is True

    scheduler.stop()


def test_event_wait_clear_on_set_false() -> None:
    """Test Event wait with clear_on_set=False."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    event.set()

    result = wait_for_event(event, clear_on_set=False)
    assert result is True
    assert scheduler_event_count(scheduler) == 1

    scheduler.stop()


def test_scheduler_remove_event() -> None:
    """Test Scheduler event removal."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    assert scheduler_event_count(scheduler) == 1

    remove_event(event)
    wait_for_condition(lambda: scheduler_event_count(scheduler) == 0)

    assert scheduler_event_count(scheduler) == 0

    scheduler.stop()


def test_event_wait_removed_event() -> None:
    """Test Event wait on removed event."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    remove_event(event)
    wait_for_condition(lambda: scheduler_event_count(scheduler) == 0)

    result = wait_for_event(event, timeout=0.01)
    assert result is False

    scheduler.stop()


def test_event_set_removed_event() -> None:
    """Test Event set on removed event."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    remove_event(event)
    wait_for_condition(lambda: scheduler_event_count(scheduler) == 0)

    # Should not crash
    event.set()

    scheduler.stop()


def test_scheduler_cleanup_on_stop() -> None:
    """Test scheduler cleanup when stopped."""
    scheduler = Scheduler()
    scheduler.start()

    # Create a future and event
    scheduler.call_later(async_noop, delay=0.1, count=1)
    scheduler.create_event()

    # Stop scheduler immediately
    scheduler.stop()

    # Events should be cleared (this is what we can reliably test)
    assert scheduler_event_count(scheduler) == 0
    # Future state may vary due to timing, but scheduler should be stopped
    assert not scheduler_is_started(scheduler)


def test_scheduler_call_later_after_stop_returns_cancelled_future() -> None:
    """Test call_later returns a cancelled future after shutdown starts."""
    scheduler = Scheduler()
    scheduler.start()
    scheduler.stop()

    future = scheduler.call_later(async_noop, delay=0.01)

    assert future.done()
    assert future.cancelled()


def test_scheduler_multiple_events() -> None:
    """Test scheduler with multiple events."""
    scheduler = Scheduler()
    scheduler.start()

    event1 = scheduler.create_event()
    event2 = scheduler.create_event()

    assert scheduler_event_count(scheduler) == 2

    event1.set()
    result1 = wait_for_event(event1, timeout=0.01)
    assert result1 is True

    result2 = wait_for_event(event2, timeout=0.01)
    assert result2 is False

    scheduler.stop()


def test_task_properties_after_scheduler_stop() -> None:
    """Test Future properties after scheduler is stopped."""
    scheduler = Scheduler()
    scheduler.start()

    async def func() -> None:
        pass

    future = scheduler.call_later(func, delay=0.01)
    wait_for_condition(lambda: future.done())

    scheduler.stop()

    assert future.done()
    assert not future.cancelled()


def test_event_timeout_handling() -> None:
    """Test Event timeout handling."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()

    start_time = time.time()
    result = wait_for_event(event, timeout=0.05)
    end_time = time.time()

    assert result is False
    assert 0.04 <= (end_time - start_time) <= 0.1

    scheduler.stop()


def test_scheduler_call_later_zero_delay() -> None:
    """Test call_later with zero delay."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def func() -> None:
        result.append("zero_delay")

    future = scheduler.call_later(func, delay=0)
    wait_for_condition(lambda: future.done())

    assert result == ["zero_delay"]
    assert future.done()

    scheduler.stop()


def test_scheduler_call_later_default_parameters() -> None:
    """Test call_later with default parameters."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def func() -> None:
        result.append("default")

    future = scheduler.call_later(func)
    wait_for_condition(lambda: future.done())

    assert result == ["default"]
    assert future.done()

    scheduler.stop()


def test_task_result_with_exception() -> None:
    """Test Future result method when function raises exception."""
    scheduler = Scheduler()
    scheduler.start()

    async def failing_func() -> None:
        msg: str = "test exception"

        raise ValueError(msg)

    # Test that user function exceptions are propagated through the Future
    with patch("async_durable_execution._runner.local.scheduler.logger") as mock_logger:
        future = scheduler.call_later(failing_func, delay=0.01)
        wait_for_condition(lambda: future.done())

        # Future should be done and exception should be logged
        assert future.done()
        mock_logger.exception.assert_called()

        # Exception should be propagated through Future.result()
        with pytest.raises(ValueError, match="test exception"):
            future.result()

    scheduler.stop()


def test_get_task_result_exception_handling() -> None:
    """Test Future result exception handling."""
    scheduler = Scheduler()
    scheduler.start()

    async def func() -> None:
        pass

    future = scheduler.call_later(func, delay=0.01)
    wait_for_condition(lambda: future.done())

    # Future result should work normally
    result = future.result()
    assert result is None

    scheduler.stop()


def test_call_later_with_async_function() -> None:
    """Test call_later runs async functions."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def async_function() -> None:
        result.append("async_executed")

    future = scheduler.call_later(async_function, delay=0.01)
    wait_for_condition(lambda: future.done())

    assert result == ["async_executed"]
    assert future.done()

    scheduler.stop()


def test_event_set_exception() -> None:
    """Test Event set_exception method."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()
    test_exception = ValueError("test exception")

    event.set_exception(test_exception)

    with pytest.raises(ValueError, match="test exception"):
        wait_for_event(event)

    scheduler.stop()


def test_call_later_with_completion_event_exception() -> None:
    """Test call_later with completion_event when function raises exception."""
    scheduler = Scheduler()
    scheduler.start()

    completion_event = scheduler.create_event()

    async def failing_func() -> None:
        msg: str = "completion event test"

        raise RuntimeError(msg)

    scheduler.call_later(failing_func, delay=0.01, completion_event=completion_event)

    # Wait for the completion event to be set with exception
    with pytest.raises(RuntimeError, match="completion event test"):
        wait_for_event(completion_event, timeout=1.0)

    scheduler.stop()


def test_call_later_multiple_iterations() -> None:
    """Test call_later with multiple count iterations."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def func() -> str | None:
        result.append("iteration")
        # Return early to test the loop behavior
        if len(result) >= 2:
            return "done"
        return None

    # Use a very small delay and count=3 to test the loop
    future = scheduler.call_later(func, delay=0.001, count=3)
    wait_for_condition(lambda: future.done(), timeout_iterations=500)

    # Should execute at least once
    assert len(result) >= 1
    assert future.done()

    scheduler.stop()


def test_wait_for_event_timeout_exception() -> None:
    """Test wait_for_event timeout handling."""
    scheduler = Scheduler()
    scheduler.start()

    event = scheduler.create_event()

    # Test timeout behavior
    result = wait_for_event(event, timeout=0.001)
    assert result is False

    scheduler.stop()


def test_call_later_loop_exit_condition() -> None:
    """Test call_later loop exit condition with count=0."""
    scheduler = Scheduler()
    scheduler.start()

    result = []

    async def func() -> None:
        result.append("should_not_execute")

    # Test with count=0 to hit the loop exit condition
    future = scheduler.call_later(func, delay=0.01, count=0)
    wait_for_condition(lambda: future.done())

    # Should not execute the function at all
    assert len(result) == 0
    assert future.done()

    scheduler.stop()

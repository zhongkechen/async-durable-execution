"""A Scheduler that can run awaitables or standard sync callables on a schedule."""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
import logging
import threading
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


class Event:
    """An event created by Scheduler that will block on wait until it's set."""

    def __init__(self, scheduler: Scheduler, event: threading.Event) -> None:
        self._scheduler: Scheduler = scheduler
        self._event: threading.Event = event
        self._exception: Exception | None = None

    def set(self):
        """Set the event with this to unblock wait."""
        self._scheduler.set_event(self._event)

    def set_exception(self, exception: Exception):
        """Set exception and unblock waiters."""
        self._exception = exception
        self._scheduler.set_event(self._event)

    def wait(self, timeout: float | None = None, *, clear_on_set: bool = True) -> bool:
        """Wait until the event is set."""
        result = self._scheduler.wait_for_event(self._event, timeout)
        if clear_on_set:
            self._scheduler.remove_event(self._event)
        if result and self._exception:
            raise self._exception
        return result

    def remove(self):
        """Remove the event from the Scheduler."""
        self._scheduler.remove_event(self._event)


class Scheduler:
    """A Scheduler to run callables later and signal events across threads."""

    def __init__(self) -> None:
        self._running: bool = False
        self._stopping: bool = False
        self._events: set[threading.Event] = set()
        self._tasks: set[Future[Any]] = set()
        self._timers: dict[Future[Any], threading.Timer] = {}
        self._lock = threading.Lock()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        """Start the scheduler. Not thread-safe."""
        if self._running:
            return
        self._running = True

    def stop(self):
        """Stop the scheduler, releasing resources. Not thread-safe."""
        if not self._running:
            return

        self._running = False
        self._stopping = True
        with self._lock:
            timers = list(self._timers.values())
            tasks = list(self._tasks)
            self._events.clear()
            self._timers.clear()
            self._tasks.clear()

        for timer in timers:
            timer.cancel()
        for task in tasks:
            task.cancel()

        self._stopping = False

    def is_started(self) -> bool:
        """Return True if the scheduler is started."""
        return self._running

    def event_count(self) -> int:
        """Return the number of events in the scheduler."""
        with self._lock:
            return len(self._events)

    def task_count(self) -> int:
        """Return the number of scheduled tasks that are not done."""
        with self._lock:
            return sum(1 for task in self._tasks if not task.done())

    def call_later(
        self,
        func: Callable[[], Any],
        delay: float = 0,
        count: int | None = 1,  # noqa: ARG002
        completion_event: Event | None = None,
    ) -> Future[Any]:
        """Call func after the delay."""
        if not self._running or self._stopping:
            cancelled_future: Future[Any] = Future()
            cancelled_future.cancel()
            return cancelled_future

        future: Future[Any] = Future()
        if count == 0:
            future.set_result(None)
            return future

        def cleanup(_future: Future[Any]) -> None:
            with self._lock:
                self._tasks.discard(_future)
                self._timers.pop(_future, None)

        def run() -> None:
            if not future.set_running_or_notify_cancel():
                return

            try:
                if asyncio.iscoroutinefunction(func):
                    result = asyncio.run(func())
                else:
                    result = func()
                future.set_result(result)
            except Exception as err:
                if completion_event:
                    completion_event.set_exception(err)
                else:
                    msg: str = "error in scheduled task"
                    logger.exception(msg)
                future.set_exception(err)

        timer = threading.Timer(delay, run)
        timer.daemon = True
        future.add_done_callback(cleanup)

        with self._lock:
            self._tasks.add(future)
            self._timers[future] = timer

        timer.start()
        return future

    def create_event(self) -> Event:
        """Create an event controlled by the Scheduler."""
        event = threading.Event()
        with self._lock:
            self._events.add(event)
        return Event(self, event)

    def wait_for_event(
        self, event: threading.Event, timeout: float | None = None
    ) -> bool:
        """Wait for an event if it is still tracked by the Scheduler."""
        with self._lock:
            if event not in self._events:
                return False
        return event.wait(timeout)

    def set_event(self, event: threading.Event):
        """Set event if it is still tracked by the Scheduler."""
        with self._lock:
            should_set = event in self._events
        if should_set:
            event.set()

    def remove_event(self, event: threading.Event):
        """Remove event from Scheduler."""
        with self._lock:
            self._events.discard(event)

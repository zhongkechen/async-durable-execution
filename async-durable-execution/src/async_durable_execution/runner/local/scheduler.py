"""A single-threaded asyncio scheduler for local runner callbacks."""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


class Event:
    """An event created by Scheduler that will block on wait until it's set."""

    def __init__(self, scheduler: Scheduler, event: asyncio.Event) -> None:
        self._scheduler: Scheduler = scheduler
        self._event: asyncio.Event = event
        self._exception: Exception | None = None

    def set(self):
        """Set the event with this to unblock wait."""
        self._scheduler.set_event(self._event)

    def set_exception(self, exception: Exception):
        """Set exception and unblock waiters."""
        self._exception = exception
        self._scheduler.set_event(self._event)

    async def wait_async(
        self, timeout: float | None = None, *, clear_on_set: bool = True
    ) -> bool:
        """Wait until the event is set."""
        result = await self._scheduler.wait_for_event(self._event, timeout)
        if clear_on_set:
            self._scheduler.remove_event(self._event)
        if result and self._exception:
            raise self._exception
        return result

    def wait(self, timeout: float | None = None, *, clear_on_set: bool = True) -> bool:
        """Synchronously wait for compatibility with scheduler unit tests."""
        loop = self._scheduler.get_loop()
        if loop.is_running():
            msg = "Event.wait() cannot block a running event loop; use wait_async()."
            raise RuntimeError(msg)
        return loop.run_until_complete(
            self.wait_async(timeout=timeout, clear_on_set=clear_on_set)
        )

    def remove(self):
        """Remove the event from the Scheduler."""
        self._scheduler.remove_event(self._event)


class Scheduler:
    """A Scheduler to run callables later on one asyncio event loop."""

    def __init__(self) -> None:
        self._running: bool = False
        self._stopping: bool = False
        self._events: set[asyncio.Event] = set()
        self._tasks: set[asyncio.Future[Any]] = set()
        self._timer_handles: dict[asyncio.Future[Any], asyncio.TimerHandle] = {}
        self._running_tasks: dict[asyncio.Future[Any], asyncio.Task[Any]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        """Start the scheduler. Not thread-safe."""
        if self._running:
            return
        self._loop = self._get_or_create_loop()
        self._running = True

    def stop(self):
        """Stop the scheduler, releasing resources. Not thread-safe."""
        if not self._running:
            return

        self._running = False
        self._stopping = True

        timer_handles = list(self._timer_handles.values())
        running_tasks = list(self._running_tasks.values())
        futures = list(self._tasks)
        self._events.clear()
        self._timer_handles.clear()
        self._running_tasks.clear()
        self._tasks.clear()

        for timer_handle in timer_handles:
            timer_handle.cancel()
        for task in running_tasks:
            task.cancel()
        for future in futures:
            future.cancel()

        self._stopping = False

    def get_loop(self) -> asyncio.AbstractEventLoop:
        """Return the scheduler event loop, creating one for sync compatibility."""
        if self._loop is None or self._loop.is_closed():
            self._loop = self._get_or_create_loop()
        return self._loop

    def is_started(self) -> bool:
        """Return True if the scheduler is started."""
        return self._running

    def event_count(self) -> int:
        """Return the number of events in the scheduler."""
        return len(self._events)

    def task_count(self) -> int:
        """Return the number of scheduled tasks that are not done."""
        return sum(1 for task in self._tasks if not task.done())

    def call_later(
        self,
        func: Callable[[], Any],
        delay: float = 0,
        count: int | None = 1,  # noqa: ARG002
        completion_event: Event | None = None,
    ) -> asyncio.Future[Any]:
        """Call func after the delay."""
        loop = self.get_loop()
        if not self._running or self._stopping:
            cancelled_future: asyncio.Future[Any] = loop.create_future()
            cancelled_future.cancel()
            return cancelled_future

        future: asyncio.Future[Any] = loop.create_future()
        if count == 0:
            future.set_result(None)
            return future

        def cleanup(_future: asyncio.Future[Any]) -> None:
            self._tasks.discard(_future)
            if timer_handle := self._timer_handles.pop(_future, None):
                timer_handle.cancel()
            if task := self._running_tasks.pop(_future, None):
                task.cancel()
            if _future.done() and not _future.cancelled():
                _future.exception()

        async def execute() -> None:
            if future.cancelled():
                return
            try:
                result = func()
                if inspect.isawaitable(result):
                    result = await result
            except Exception as err:
                if completion_event:
                    completion_event.set_exception(err)
                else:
                    msg: str = "error in scheduled task"
                    logger.exception(msg)
                if not future.done():
                    future.set_exception(err)
            else:
                if not future.done():
                    future.set_result(result)

        def run() -> None:
            if not self._running or self._stopping or future.cancelled():
                return
            task = loop.create_task(execute())
            self._running_tasks[future] = task

            def discard_task(_task: asyncio.Task[Any]) -> None:
                self._running_tasks.pop(future, None)

            task.add_done_callback(discard_task)

        future.add_done_callback(cleanup)
        timer_handle = loop.call_later(delay, run)

        self._tasks.add(future)
        self._timer_handles[future] = timer_handle
        return future

    def create_event(self) -> Event:
        """Create an event controlled by the Scheduler."""
        event = asyncio.Event()
        self._events.add(event)
        return Event(self, event)

    async def wait_for_event(
        self, event: asyncio.Event, timeout: float | None = None
    ) -> bool:
        """Wait for an event if it is still tracked by the Scheduler."""
        if event not in self._events:
            return False
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return event in self._events

    def set_event(self, event: asyncio.Event):
        """Set event if it is still tracked by the Scheduler."""
        should_set = event in self._events
        if should_set:
            event.set()

    def remove_event(self, event: asyncio.Event):
        """Remove event from Scheduler."""
        self._events.discard(event)
        event.set()

    @staticmethod
    def _get_or_create_loop() -> asyncio.AbstractEventLoop:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            pass

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop

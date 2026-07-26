from __future__ import annotations

from collections.abc import Callable
import contextvars
import threading
from typing import Any

import asyncio

import pytest


@pytest.fixture(autouse=True)
def default_to_async_lambda_client(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """Keep tests deterministic when optional async dependencies are missing."""
    marker = request.node.get_closest_marker("aioboto_installed")
    installed = True if marker is None else bool(marker.args[0])
    monkeypatch.setattr(
        "async_durable_execution.core.client.aioboto_is_installed", lambda: installed
    )


@pytest.fixture(autouse=True)
def run_to_thread_without_default_executor(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid default executor shutdown hangs in unit tests."""

    async def isolated_to_thread(
        func: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> Any:
        context = contextvars.copy_context()
        result: list[Any] = []
        errors: list[BaseException] = []

        def run() -> None:
            try:
                result.append(context.run(func, *args, **kwargs))
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=run)
        thread.start()
        while thread.is_alive():
            await asyncio.sleep(0.001)
        thread.join()
        if errors:
            raise errors[0]
        return result[0] if result else None

    monkeypatch.setattr(asyncio, "to_thread", isolated_to_thread)

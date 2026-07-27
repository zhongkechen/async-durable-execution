import importlib.util
from collections.abc import Awaitable, Callable
from pathlib import Path
from unittest.mock import Mock

import pytest


HANDLER_PATH = (
    Path(__file__).resolve().parents[2]
    / "conformance"
    / "child"
    / "child_interrupted.py"
)
handler_spec = importlib.util.spec_from_file_location("child_interrupted", HANDLER_PATH)
assert handler_spec is not None
assert handler_spec.loader is not None
child_interrupted = importlib.util.module_from_spec(handler_spec)
handler_spec.loader.exec_module(child_interrupted)


@pytest.mark.parametrize(
    ("is_replaying", "should_crash"),
    [
        (False, True),
        (True, False),
    ],
)
async def test_interrupted_child_crashes_only_before_replay(
    monkeypatch: pytest.MonkeyPatch,
    *,
    is_replaying: bool,
    should_crash: bool,
) -> None:
    context = Mock()
    context.is_replaying.return_value = is_replaying
    monkeypatch.setattr(child_interrupted, "get_durable_context", lambda: context)

    def fake_crashable_step(
        *,
        should_crash: bool,
        value: str,
    ) -> Callable[[], Awaitable[tuple[bool, str]]]:
        async def execute() -> tuple[bool, str]:
            return should_crash, value

        return execute

    async def fake_step(
        func: Callable[[], Awaitable[tuple[bool, str]]],
        **_: object,
    ) -> tuple[bool, str]:
        return await func()

    monkeypatch.setattr(child_interrupted, "crashable_step", fake_crashable_step)
    monkeypatch.setattr(child_interrupted, "step", fake_step)

    result = await child_interrupted.interrupted_child(value="result")()

    assert result == (should_crash, "result")

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def default_to_sync_lambda_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep tests deterministic when optional async dependencies are installed."""
    monkeypatch.setattr(
        "async_durable_execution.client.aioboto_is_installed", lambda: False
    )

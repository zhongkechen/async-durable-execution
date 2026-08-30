"""End-to-end tests for filesystem-backed SerDes pipelines."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from async_durable_execution import InvocationStatus, OperationStatus
from examples.filesystem_serdes import (
    filesystem_serdes_always,
    filesystem_serdes_overflow,
)

_FILESYSTEM_MARKER = "__durable_execution_filesystem_serdes"
_CLOUD_MOUNT_ENV = "FILESYSTEM_SERDES_CLOUD_MOUNT_PATH"


@pytest.fixture
def filesystem_mount_path(request: pytest.FixtureRequest, tmp_path: Path) -> str:
    """Use temporary local storage or an explicitly configured cloud mount."""
    if request.config.getoption("--runner-mode") != "cloud":
        return str(tmp_path)

    mount_path = os.environ.get(_CLOUD_MOUNT_ENV)
    if mount_path:
        return mount_path
    pytest.skip(
        f"Cloud filesystem SerDes tests require {_CLOUD_MOUNT_ENV} to point "
        "to an EFS or S3 Files mount configured on the deployed functions."
    )


def _step_envelope(result: Any, name: str) -> dict[str, Any]:
    operation = result.get_step(name)
    assert operation.status is OperationStatus.SUCCEEDED
    assert operation.step_details is not None
    assert operation.step_details.result is not None
    envelope = json.loads(operation.step_details.result)
    assert envelope[_FILESYSTEM_MARKER] == 1
    return envelope


def _is_cloud(request: pytest.FixtureRequest) -> bool:
    return request.config.getoption("--runner-mode") == "cloud"


async def test_filesystem_serdes_always_round_trips_across_replay(
    durable_runner,
    filesystem_mount_path: str,
    request: pytest.FixtureRequest,
) -> None:
    """Persist every step result, replay it from storage, and preserve previews."""
    async with durable_runner(
        handler=filesystem_serdes_always.handler,
        input={
            "mount_path": filesystem_mount_path,
            "order_id": "order-e2e-1",
        },
        timeout=30,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "success": True,
        "order_id": "order-e2e-1",
        "item_count": 12,
        "total_quantity": 78,
        "first_sku": "item-00",
        "last_sku": "item-11",
    }
    assert result.get_wait("filesystem-replay-boundary").status is (
        OperationStatus.SUCCEEDED
    )

    order_envelope = _step_envelope(result, "persist-order")
    summary_envelope = _step_envelope(result, "summarize-order")
    assert order_envelope["preview"] == {
        "order_id": "order-e2e-1",
        "customer_email": "***",
    }
    assert "file" in order_envelope
    assert "file" in summary_envelope
    assert order_envelope["file"] != summary_envelope["file"]

    if not _is_cloud(request):
        order_file = Path(order_envelope["file"])
        summary_file = Path(summary_envelope["file"])
        assert order_file.is_file()
        assert summary_file.is_file()
        order_payload = order_file.read_bytes()
        order_digest = hashlib.sha256(order_payload).hexdigest()
        assert order_digest == order_envelope["payloadDigest"]
        assert len(order_payload) == order_envelope["payloadSizeBytes"]
        stored_order = json.loads(order_payload)
        assert stored_order["order_id"] == "order-e2e-1"
        assert len(stored_order["items"]) == 12


async def test_filesystem_serdes_overflow_replays_inline_and_file_payloads(
    durable_runner,
    filesystem_mount_path: str,
    request: pytest.FixtureRequest,
) -> None:
    """Keep the small result inline and restore the large result from a file."""
    async with durable_runner(
        handler=filesystem_serdes_overflow.handler,
        input={"mount_path": filesystem_mount_path},
        timeout=45,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "small_status": "inline",
        "small_count": 3,
        "large_record_count": 300,
        "first_record_id": 0,
        "last_record_id": 299,
        "payload_length": 1035,
    }
    assert result.get_wait("overflow-replay-boundary").status is (
        OperationStatus.SUCCEEDED
    )

    inline_envelope = _step_envelope(result, "inline-payload")
    file_envelope = _step_envelope(result, "file-payload")
    assert "data" in inline_envelope
    assert "file" not in inline_envelope
    assert json.loads(inline_envelope["data"]) == {
        "status": "inline",
        "count": 3,
    }
    assert "file" in file_envelope
    assert "data" not in file_envelope

    if not _is_cloud(request):
        payload_file = Path(file_envelope["file"])
        assert payload_file.is_file()
        payload = payload_file.read_bytes()
        assert hashlib.sha256(payload).hexdigest() == file_envelope["payloadDigest"]
        assert len(payload) == file_envelope["payloadSizeBytes"]
        stored_result = json.loads(payload)
        assert len(stored_result["records"]) == 300
        assert stored_result["records"][299]["id"] == 299

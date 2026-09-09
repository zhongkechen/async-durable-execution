"""Keep small checkpoints inline and offload large checkpoints to a filesystem."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from async_durable_execution import (
    FileSystemSerDesMode,
    FileSystemSerDesStage,
    FileSystemSerDesStageConfig,
    JsonSerDes,
    durable_callable,
    durable_execution,
    step,
    wait,
)


@durable_callable
async def build_small_result() -> dict[str, Any]:
    """Return a result small enough to remain in its checkpoint envelope."""
    return {"status": "inline", "count": 3}


@durable_callable
async def build_large_result() -> dict[str, Any]:
    """Return a result large enough to overflow to the filesystem."""
    return {
        "records": [
            {
                "id": index,
                "payload": f"record-{index:03d}-" + ("x" * 1024),
            }
            for index in range(300)
        ]
    }


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, Any]:
    """Replay both inline and file-backed overflow checkpoints."""
    serdes = JsonSerDes[dict[str, Any]]().then(
        FileSystemSerDesStage(
            event["mount_path"],
            FileSystemSerDesStageConfig(
                storage_mode=FileSystemSerDesMode.OVERFLOW,
            ),
        )
    )

    small_result = await step(
        build_small_result(),
        name="inline-payload",
        serdes=serdes,
    )
    large_result = await step(
        build_large_result(),
        name="file-payload",
        serdes=serdes,
    )

    # Both completed steps are restored from their checkpoints after this wait.
    await wait(
        duration=timedelta(seconds=1),
        name="overflow-replay-boundary",
    )

    records = large_result["records"]
    return {
        "small_status": small_result["status"],
        "small_count": small_result["count"],
        "large_record_count": len(records),
        "first_record_id": records[0]["id"],
        "last_record_id": records[-1]["id"],
        "payload_length": len(records[0]["payload"]),
    }

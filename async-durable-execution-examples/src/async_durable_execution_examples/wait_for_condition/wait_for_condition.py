"""Example demonstrating wait-for-condition pattern."""

import asyncio
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_execution,
    get_current_context,
    JitterStrategy,
    SerDes,
    WaitDelayStrategy,
    WaitForConditionCheckContext,
    wait_for_condition,
)


@dataclass(frozen=True)
class JobStatus:
    """Custom check result that controls wait completion through truthiness."""

    job_id: str
    attempts: int
    status: str

    def __bool__(self) -> bool:
        return self.status == "completed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "attempts": self.attempts,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobStatus":
        return cls(
            job_id=str(data["job_id"]),
            attempts=int(data["attempts"]),
            status=str(data["status"]),
        )


class JobStatusSerDes(SerDes[JobStatus]):
    """Serialize the custom wait_for_condition check result."""

    async def serialize(self, value: JobStatus) -> str:
        return json.dumps(value.to_dict())

    async def deserialize(self, data: str) -> JobStatus:
        return JobStatus.from_dict(json.loads(data))


@durable_execution
async def handler(_event: Any) -> dict[str, Any]:
    """Handler demonstrating wait-for-condition pattern."""

    async def check_function(_state: JobStatus | None) -> JobStatus:
        """Return a JobStatus whose __bool__ determines polling completion."""
        await asyncio.sleep(0)
        context = get_current_context()
        assert isinstance(context, WaitForConditionCheckContext)
        attempt = context.attempt or 1
        return JobStatus(
            job_id="job-123",
            attempts=attempt,
            status="completed" if attempt >= 3 else "pending",
        )

    wait_strategy = WaitDelayStrategy[JobStatus](
        initial_delay=timedelta(seconds=1),
        jitter_strategy=JitterStrategy.NONE,
    )

    result = await wait_for_condition(
        check=check_function,
        initial_state=None,
        wait_strategy=wait_strategy,
        serdes=JobStatusSerDes(),
    )

    return result.to_dict()

"""The custom polling result retains its type and attempt across durable retries."""

from async_durable_execution import InvocationStatus
from examples.wait_for_condition import wait_for_condition


async def test_wait_for_condition(durable_runner) -> None:
    async with durable_runner(
        handler=wait_for_condition.handler, input="test", timeout=30
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == {
        "job_id": "job-123",
        "attempts": 3,
        "status": "completed",
    }

"""Tests for map with custom should_complete decisions."""

from async_durable_execution import InvocationStatus
from async_durable_execution import OperationStatus
from examples.map import map_with_custom_should_complete


async def test_map_with_custom_should_complete_succeeds(durable_runner) -> None:
    """Custom completion can stop the map after enough successful items."""
    async with durable_runner(
        handler=map_with_custom_should_complete.handler,
        input={
            "providers": ["primary", "secondary", "slow-tertiary"],
            "required_successes": 2,
            "failure_limit": 2,
        },
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()
    assert result_data["completion_reason"] == "CUSTOM_COMPLETION_SUCCEEDED"
    assert result_data["completion_succeeded"] is True
    assert result_data["responses"] == ["response:primary", "response:secondary"]
    assert result_data["success_count"] == 2
    assert result_data["failure_count"] == 0
    assert result_data["started_count"] == 0
    assert result_data["total_count"] == 2

    map_op = result.get_context("custom_should_complete_map")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED


async def test_map_with_custom_should_complete_can_complete_as_failed(
    durable_runner,
) -> None:
    """Custom completion can stop the map with failed completion semantics."""
    async with durable_runner(
        handler=map_with_custom_should_complete.handler,
        input={
            "providers": ["bad-primary", "bad-secondary", "slow-tertiary"],
            "required_successes": 2,
            "failure_limit": 2,
        },
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()
    assert result_data["completion_reason"] == "CUSTOM_COMPLETION_FAILED"
    assert result_data["completion_succeeded"] is False
    assert result_data["responses"] == []
    assert result_data["success_count"] == 0
    assert result_data["failure_count"] == 2
    assert result_data["started_count"] == 0
    assert result_data["total_count"] == 2

    map_op = result.get_context("custom_should_complete_map")
    assert map_op is not None
    assert map_op.status is OperationStatus.SUCCEEDED

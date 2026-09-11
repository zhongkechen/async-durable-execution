"""Tests for many async durable steps example."""

from async_durable_execution import InvocationStatus, OperationType
from examples.step import many_async_steps


async def test_many_async_steps(durable_runner) -> None:
    """Test many step tasks collected with asyncio.gather."""
    async with durable_runner(
        handler=many_async_steps.handler,
        input={"multiplier": 2, "steps": 500},
        timeout=20,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()
    assert result_data["result"] == 249500


async def test_many_async_steps_with_multiplier_one(durable_runner) -> None:
    """Test the 500-step sum with multiplier one."""
    async with durable_runner(
        handler=many_async_steps.handler,
        input={"multiplier": 1, "steps": 500},
        timeout=20,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result()["result"] == 124750


async def test_many_async_steps_operations_are_tracked(durable_runner) -> None:
    """Test representative compute operations and replay operations are tracked."""
    async with durable_runner(
        handler=many_async_steps.handler,
        input={"multiplier": 1, "steps": 500},
        timeout=20,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    for index in (0, 250):
        assert result.get_operation_by_name(f"compute-{index}") is not None

    result_data = result.get_deserialized_result()
    assert isinstance(result_data["execution_time_ms"], int)
    assert isinstance(result_data["replay_time_ms"], int)
    assert result_data["replay_time_ms"] >= result_data["execution_time_ms"]

    assert result.get_step("start-time").operation_type is OperationType.STEP
    assert result.get_step("execution-time").operation_type is OperationType.STEP
    assert result.get_step("replay-time").operation_type is OperationType.STEP
    assert result.get_wait("post-compute-wait").operation_type is OperationType.WAIT

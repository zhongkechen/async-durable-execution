"""Tests for wait_for_condition."""


async def test_wait_for_condition(durable_runner) -> None:
    """Test wait_for_condition pattern."""
    # TODO: fix bug in local runner so that local tests can pass
    # async with durable_runner(
    #     handler=wait_for_condition.handler,
    #     input="test",
    #     timeout=30,
    # ) as runner:
    #     result = await runner.run()

    # assert result.status is InvocationStatus.SUCCEEDED
    # # Should reach state 3 after 3 increments
    # assert result.get_deserialized_result() == 3

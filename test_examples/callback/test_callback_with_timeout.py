"""Creating a callback exposes its identity without awaiting external completion."""

from async_durable_execution import InvocationStatus
from examples.callback import callback_with_timeout


async def test_callback_with_timeout(durable_runner) -> None:
    async with durable_runner(
        handler=callback_with_timeout.handler, timeout=30
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    callback = result.get_callback("timeout_callback")
    assert callback.callback_details.callback_id
    assert result.get_deserialized_result() == (
        f"Callback created with 60s timeout: {callback.callback_details.callback_id}"
    )

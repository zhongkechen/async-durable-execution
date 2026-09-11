"""Local callback payloads mirror AWS service results, including replay."""

import pytest

from async_durable_execution import (
    Callback,
    InvocationStatus,
    JsonSerDes,
    create_callback,
    create_local_runner,
    durable_execution,
    wait,
)


@pytest.mark.parametrize(
    "payload, decoded, use_codec",
    [
        (None, None, False),
        (b"", None, False),
        (None, None, True),
        (b"", None, True),
        (b'""', "", True),
        (b"false", False, True),
        (b" ", " ", False),
    ],
)
async def test_callback_payload_result_survives_replay(payload, decoded, use_codec):
    observed = []

    @durable_execution
    async def handler(event):
        callback: Callback = await create_callback(
            name="approval", serdes=JsonSerDes() if use_codec else None
        )
        value = await callback.result()
        observed.append(value)
        await wait(1, name="replay")
        return value

    async with create_local_runner(handler=handler, timeout=3) as runner:
        arn = await runner.run_async()
        callback_id = await runner.wait_for_callback(arn, name="approval")
        await runner.send_callback_success(callback_id, payload)
        result = await runner.wait_for_result(arn)

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == decoded
    assert observed == [decoded, decoded]
    assert result.get_callback("approval").callback_details.result == (
        payload.decode() if payload else None
    )

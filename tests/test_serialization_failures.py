"""Persisted values must fail predictably when codecs or payloads are invalid."""

import json

import pytest

from async_durable_execution import (
    ExtendedTypeSerDes,
    FileSystemSerDesMode,
    FileSystemSerDesStage,
    FileSystemSerDesStageConfig,
    InvocationStatus,
    JsonSerDes,
    RetryableSerDesError,
    RetryStrategy,
    SerDesContext,
    SerDesError,
    SerDesPipelineError,
    create_local_runner,
    create_serdes_pipeline,
    durable_execution,
    get_serdes_context,
    get_step_context,
    step,
    wait,
)


@pytest.mark.parametrize("action", ["serialize", "deserialize"])
@pytest.mark.parametrize("stage_index", [0, 1, 2])
@pytest.mark.parametrize("retryable", [False, True])
async def test_pipeline_errors_preserve_component_cause_and_retryability(
    action, stage_index, retryable
):
    calls = []
    failure = (
        RetryableSerDesError("storage unavailable")
        if retryable
        else ValueError("bad value")
    )

    class Component:
        def __init__(self, index):
            self.index = index

        async def serialize(self, value, context=None):
            calls.append(self.index)
            if self.index == stage_index:
                raise failure
            return value

        deserialize = serialize

    components = [Component(index) for index in range(3)]
    pipeline = create_serdes_pipeline(*components)
    expected_error = RetryableSerDesError if retryable else SerDesPipelineError
    with pytest.raises(expected_error) as caught:
        await getattr(pipeline, action)("payload")

    assert calls == (
        list(range(stage_index + 1))
        if action == "serialize"
        else list(range(2, stage_index - 1, -1))
    )
    if retryable:
        assert caught.value is failure
    else:
        assert isinstance(caught.value, SerDesPipelineError)
        assert caught.value.stage_index == stage_index
        assert caught.value.action == action
        assert caught.value.stage is components[stage_index]
        assert caught.value.__cause__ is failure


async def test_transient_serialization_failure_retries_with_operation_metadata():
    attempts = []
    writes = []
    reads = []

    class Storage(JsonSerDes):
        async def serialize(self, value):
            context = get_serdes_context()
            writes.append(context)
            if len(writes) == 1:
                raise RetryableSerDesError("temporary storage outage")
            return await super().serialize(value)

        async def deserialize(self, data):
            reads.append(get_serdes_context())
            return await super().deserialize(data)

    async def work():
        attempts.append(get_step_context().attempt)
        return {"value": 42}

    @durable_execution
    async def handler(event):
        value = await step(
            work,
            name="stored",
            serdes=Storage(),
            retry_strategy=RetryStrategy(max_attempts=2, initial_delay=1),
        )
        await wait(1)
        return value

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == {"value": 42}
    assert attempts == [1, 2]
    assert [ctx.attempt for ctx in writes] == [1, 2]
    assert [ctx.attempt for ctx in reads] == [2, 2]
    assert all(ctx.original_value == {"value": 42} for ctx in writes)
    assert all(ctx.original_value is None for ctx in reads)
    assert {ctx.operation_id for ctx in writes + reads} == {
        result.get_step("stored").operation_id
    }
    assert {ctx.operation_name for ctx in writes + reads} == {"stored"}


@pytest.mark.parametrize("invalid_output", [False, True])
async def test_permanent_serialization_failure_does_not_retry_user_effect(
    invalid_output,
):
    calls = []

    class BadStorage(JsonSerDes):
        async def serialize(self, value):
            if invalid_output:
                return 42
            raise ValueError("cannot serialize the result")

    async def work():
        calls.append("effect")
        return 1

    @durable_execution
    async def handler(event):
        return await step(work, serdes=BadStorage(), name="bad-storage")

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.FAILED
    assert result.error.type == "SerDesError"
    assert calls == ["effect"]
    assert (
        "Serializers must return a string" if invalid_output else "cannot serialize"
    ) in result.error.message


@pytest.mark.parametrize("value", [object(), {1, 2}, {"nested": object()}])
async def test_unsupported_extended_values_are_rejected(value):
    codec: ExtendedTypeSerDes = ExtendedTypeSerDes()
    with pytest.raises(SerDesError, match="Unsupported serialized value"):
        await codec.serialize(value)


@pytest.mark.parametrize("data", ["", "{", '{"key":', "[1,]", "undefined"])
async def test_malformed_json_is_a_serialization_error(data):
    with pytest.raises(SerDesError, match="Cannot deserialize"):
        await ExtendedTypeSerDes().deserialize(data)


@pytest.mark.parametrize(
    "field, value",
    [
        ("adeFS", 4),
        ("adeFS", True),
        ("bytes", -1),
        ("bytes", True),
        ("bytes", 999),
        ("sha256", "z" * 64),
        ("sha256", "0" * 64),
        ("owner", None),
        ("entity", None),
        ("data", "modified"),
        ("data", 42),
        ("unexpected", "field"),
    ],
)
async def test_inline_filesystem_metadata_and_integrity_are_validated(
    tmp_path, field, value
):
    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(storage_mode=FileSystemSerDesMode.OVERFLOW),
    )
    context = SerDesContext("operation", "execution")
    original = await stage.serialize("original", context)
    envelope = json.loads(original)
    assert "data" in envelope
    envelope[field] = value

    with pytest.raises(SerDesError):
        await stage.deserialize(json.dumps(envelope), context)
    assert await stage.deserialize(original, context) == "original"
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("invalid", ['{"adeFS":', '{"adeFS":3,"adeFS":3}'])
async def test_malformed_filesystem_envelope_is_not_treated_as_plain_text(
    tmp_path, invalid
):
    stage = FileSystemSerDesStage(tmp_path)
    with pytest.raises(SerDesError, match="Malformed filesystem envelope"):
        await stage.deserialize(invalid, SerDesContext("operation", "execution"))


async def test_filesystem_preview_failure_does_not_publish_payload(tmp_path):
    stage = FileSystemSerDesStage(
        tmp_path,
        FileSystemSerDesStageConfig(generate_preview=lambda value, context: [value]),
    )
    with pytest.raises(SerDesError, match="Preview generator must return a dictionary"):
        await stage.serialize("payload", SerDesContext("operation", "execution"))
    assert list(tmp_path.iterdir()) == []

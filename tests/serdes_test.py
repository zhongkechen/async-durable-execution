from typing import no_type_check
import asyncio
import base64
import json
import math
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from async_durable_execution._core.context import (
    bind_current_context,
    get_current_context,
    reset_current_context,
    set_current_context,
)
from async_durable_execution._core.exceptions import (
    DurableExecutionsError,
    ExecutionError,
    SerDesError,
)
from async_durable_execution._core.serdes import (
    BytesCodec,
    ContainerCodec,
    DateTimeCodec,
    DecimalCodec,
    EncodedValue,
    ExtendedTypeSerDes,
    JsonSerDes,
    PassThroughSerDes,
    PrimitiveCodec,
    SerDes,
    SerDesContext,
    TypeCodec,
    TypeTag,
    UuidCodec,
    deserialize,
    get_serdes_context,
    serialize,
)


# Custom SerDes implementation for testing
class CustomStrSerDes(SerDes[str]):
    async def serialize(self, value: str) -> str:
        return value.upper()

    async def deserialize(self, data: str) -> str:
        return data.lower()


class CustomDictSerDes(SerDes[Any]):
    async def serialize(self, value: Any) -> str:
        transformed = self._rec_serialize(value)
        return json.dumps(transformed)

    def _rec_serialize(self, value: Any) -> Any:
        if isinstance(value, dict):
            transformed = value.copy()
            for k, v in transformed.items():
                transformed[k] = self._rec_serialize(v)
            return transformed
        if isinstance(value, str):
            return value.upper()
        if isinstance(value, int):
            return str(value * 2)
        return value

    async def deserialize(self, data: str) -> dict[str, Any]:
        parsed = json.loads(data)
        return self._rec_deserialize(parsed)

    def _rec_deserialize(self, value: Any) -> Any:
        if isinstance(value, dict):
            transformed = value.copy()
            for k, v in transformed.items():
                transformed[k] = self._rec_deserialize(v)
            return transformed
        if isinstance(value, str) and value.isdigit():
            return int(value) // 2
        if isinstance(value, str):
            return value.lower()
        return value


async def test_serdes_abstract() -> None:
    """Test SerDes abstract base class."""

    class TestSerDes(SerDes):
        async def serialize(self, value) -> Any:
            return str(value)

        async def deserialize(self, data) -> Any:
            return data

    serdes = TestSerDes()
    assert await serdes.serialize(42) == "42"
    assert await serdes.deserialize("test") == "test"


@no_type_check
async def test_serdes_abstract_methods() -> None:
    """Test SerDes abstract methods must be implemented."""
    with pytest.raises(TypeError):
        SerDes()


@no_type_check
async def test_serdes_abstract_methods_not_implemented() -> None:
    """Test SerDes abstract methods raise NotImplementedError when not overridden."""

    class IncompleteSerDes(SerDes):
        pass

    # This should raise TypeError because abstract methods are not implemented
    with pytest.raises(TypeError):
        IncompleteSerDes()


@no_type_check
async def test_serdes_abstract_methods_coverage() -> None:
    """Test to achieve coverage of abstract method pass statements."""
    # To cover the pass statements, call the abstract methods directly
    assert SerDes.serialize(None, None) is None
    assert SerDes.deserialize(None, None) is None


@no_type_check
async def test_serialize_invalid_json() -> None:
    circular_ref = {"a": 1}
    circular_ref["self"] = circular_ref

    with pytest.raises(ExecutionError) as exc_info:
        await serialize(None, circular_ref, "test-op", "test-arn")
    assert "Serialization failed" in str(exc_info.value)


async def test_deserialize_invalid_json() -> None:
    with pytest.raises(ExecutionError) as exc_info:
        await deserialize(None, "invalid json", "test-op", "test-arn")
    assert "Deserialization failed" in str(exc_info.value)


@no_type_check
async def test_none_serdes_context() -> None:
    data = {"test": "value"}
    result = await serialize(None, data, None, None)
    # Dict uses envelope format, so roundtrip through deserialize
    deserialized = await deserialize(None, result, None, None)
    assert deserialized == data


@no_type_check
async def test_default_json_serialization() -> None:
    data = {"name": "test", "value": 123}
    serialized = await serialize(None, data, "test-op", "test-arn")
    assert isinstance(serialized, str)
    # Dict uses envelope format, so roundtrip through deserialize
    deserialized = await deserialize(None, serialized, "test-op", "test-arn")
    assert deserialized == data


@no_type_check
async def test_default_json_deserialization() -> None:
    # Use a simple list that can be plain JSON
    data = "[1, 2, 3]"
    deserialized = await deserialize(None, data, "test-op", "test-arn")
    assert isinstance(deserialized, list)
    assert deserialized == [1, 2, 3]


@no_type_check
async def test_default_json_roundtrip() -> None:
    original = {"name": "test", "value": 123}
    serialized = await serialize(None, original, "test-op", "test-arn")
    deserialized = await deserialize(None, serialized, "test-op", "test-arn")
    assert deserialized == original


async def test_custom_str_serdes_serialization() -> None:
    result = await serialize(CustomStrSerDes(), "hello world", "test-op", "test-arn")
    assert result == "HELLO WORLD"


async def test_custom_str_serdes_deserialization() -> None:
    result = await deserialize(CustomStrSerDes(), "HELLO WORLD", "test-op", "test-arn")
    assert result == "hello world"


async def test_custom_str_serdes_roundtrip() -> None:
    original = "hello world"
    serialized = await serialize(CustomStrSerDes(), original, "test-op", "test-arn")
    deserialized = await deserialize(
        CustomStrSerDes(), serialized, "test-op", "test-arn"
    )
    assert deserialized == "hello world"


async def test_custom_dict_serdes_serialization() -> None:
    serdes = CustomDictSerDes()
    original = {"name": "test", "value": 123}
    serialized = await serialize(serdes, original, "test-op", "test-arn")
    assert serialized == '{"name": "TEST", "value": "246"}'
    deserialized = await deserialize(serdes, serialized, "test-op", "test-arn")
    assert deserialized == original


async def test_empty_string_serialization() -> None:
    result = await serialize(None, "", "test-op", "test-arn")
    assert result == '""'


@no_type_check
async def test_empty_string_deserialization() -> None:
    result = await deserialize(None, '""', "test-op", "test-arn")
    assert not result


@no_type_check
async def test_none_value_handling() -> None:
    result = await serialize(None, None, "test-op", "test-arn")
    assert result == "null"
    deserialized = await deserialize(None, "null", "test-op", "test-arn")
    assert deserialized is None


async def test_context_propagation() -> None:
    class ContextCheckingSerDes(SerDes[str]):
        async def serialize(self, value: str) -> str:
            serdes_context = get_current_context()
            assert serdes_context.operation_id == "test-op"
            assert serdes_context.durable_execution_arn == "test-arn"
            return value + serdes_context.durable_execution_arn

        async def deserialize(self, data: str) -> str:
            serdes_context = get_current_context()
            assert serdes_context.operation_id == "test-op"
            assert serdes_context.durable_execution_arn == "test-arn"
            return data + serdes_context.operation_id

    serdes = ContextCheckingSerDes()
    data = "data"
    serialized = await serialize(serdes, data, "test-op", "test-arn")
    assert serialized == "data" + "test-arn"
    deserialized = await deserialize(serdes, serialized, "test-op", "test-arn")
    assert deserialized == "data" + "test-arn" + "test-op"


def test_get_serdes_context_returns_bound_serdes_context() -> None:
    context = SerDesContext("test-op", "test-arn")

    with bind_current_context(context):
        assert get_serdes_context() is context


@no_type_check
def test_get_serdes_context_rejects_non_serdes_context() -> None:
    with (
        bind_current_context(object()),
        pytest.raises(
            RuntimeError,
            match=r"get_serdes_context\(\) can only be used while a SerDes operation is executing\.",
        ),
    ):
        get_serdes_context()


async def test_serdes_context_exposes_recursive_level() -> None:
    class RecursiveLevelSerDes(SerDes[str]):
        async def serialize(self, value: str) -> str:
            serdes_context = get_current_context()
            return f"{value}:{serdes_context.recursive_level}"

        async def deserialize(self, data: str) -> str:
            serdes_context = get_current_context()
            return f"{data}:{serdes_context.recursive_level}"

    serdes = RecursiveLevelSerDes()

    serialized = await serialize(
        serdes,
        "payload",
        "test-op",
        "test-arn",
        recursive_level=4,
    )
    assert serialized == "payload:4"

    deserialized = await deserialize(
        serdes,
        serialized,
        "test-op",
        "test-arn",
        recursive_level=4,
    )
    assert deserialized == "payload:4:4"


async def test_context_restored_after_serdes_operation() -> None:
    previous_context = object()
    token = set_current_context(previous_context)
    try:
        serialized = await serialize(None, {"value": 42}, "test-op", "test-arn")
        assert get_current_context() is previous_context

        await deserialize(None, serialized, "test-op", "test-arn")
        assert get_current_context() is previous_context
    finally:
        reset_current_context(token)


async def test_async_serdes_can_await_io_like_work() -> None:
    class AsyncContextSerDes(SerDes[str]):
        async def serialize(self, value: str) -> str:
            await asyncio.sleep(0)
            serdes_context = get_current_context()
            return f"{serdes_context.operation_id}:{value}"

        async def deserialize(self, data: str) -> str:
            await asyncio.sleep(0)
            serdes_context = get_current_context()
            return f"{data}:{serdes_context.durable_execution_arn}"

    serialized = await serialize(AsyncContextSerDes(), "payload", "op-1", "arn-1")
    assert serialized == "op-1:payload"

    deserialized = await deserialize(AsyncContextSerDes(), serialized, "op-1", "arn-1")
    assert deserialized == "op-1:payload:arn-1"


async def test_synchronous_serdes_methods_run_with_context():
    class SyncContextSerDes(SerDes[str]):
        def serialize(self, value: str) -> str:
            context = get_serdes_context()
            return f"{context.operation_id}:{value}"

        def deserialize(self, data: str) -> str:
            context = get_serdes_context()
            return f"{data}:{context.durable_execution_arn}"

    serialized = await serialize(SyncContextSerDes(), "payload", "op-1", "arn-1")
    assert serialized == "op-1:payload"

    deserialized = await deserialize(SyncContextSerDes(), serialized, "op-1", "arn-1")
    assert deserialized == "op-1:payload:arn-1"


async def _roundtrip_envelope(value: Any) -> Any:
    """Helper for envelope round-trip testing."""
    serdes: ExtendedTypeSerDes[Any] = ExtendedTypeSerDes()
    context = SerDesContext(
        "test-op", "arn:aws:lambda:us-east-1:123456789012:function:test"
    )
    serialized = await serdes.serialize(value)
    return await serdes.deserialize(serialized)


async def test_envelope_none_roundtrip() -> None:
    assert await _roundtrip_envelope(None) is None


async def test_envelope_bool_roundtrip() -> None:
    assert await _roundtrip_envelope(True) is True
    assert await _roundtrip_envelope(False) is False


async def test_envelope_int_roundtrip() -> None:
    values = [0, 1, -1, 42, -999, 2**63 - 1, -(2**63)]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_float_roundtrip() -> None:
    values = [0.0, 1.5, -math.pi, 1e10, -1e-10, float("inf"), float("-inf")]
    for val in values:
        result = await _roundtrip_envelope(val)
        if val != val:  # NaN check  # noqa: PLR0124
            assert result != result  # NaN != NaN  # noqa: PLR0124
        else:
            assert result == val


async def test_envelope_float_nan_roundtrip() -> None:
    nan_val = float("nan")
    result = await _roundtrip_envelope(nan_val)
    assert result != result  # NaN != NaN is True  # noqa: PLR0124


async def test_envelope_str_roundtrip() -> None:
    values = ["", "hello", "🚀", "line1\nline2", "tab\there", '"quotes"', "\\backslash"]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_datetime_roundtrip() -> None:
    values = [
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
        datetime(1970, 1, 1, tzinfo=timezone.utc),
        datetime.now(timezone.utc),
        datetime.now(timezone.utc),
    ]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_date_roundtrip() -> None:
    values = [
        date(2024, 1, 1),
        date(1970, 1, 1),
        date(9999, 12, 31),
        date.today(),  # noqa: DTZ011
    ]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_decimal_roundtrip() -> None:
    values = [
        Decimal(0),
        Decimal("3.14159"),
        Decimal("-999.999"),
        Decimal("1e10"),
        Decimal("1e-28"),
        Decimal("123456789.123456789"),
    ]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_uuid_roundtrip() -> None:
    values = [
        uuid.uuid4(),
        uuid.UUID("12345678-1234-5678-1234-123456789abc"),
        uuid.UUID(int=0),
        uuid.UUID(int=2**128 - 1),
    ]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_bytes_roundtrip() -> None:
    values = [
        b"",
        b"hello",
        b"\x00\x01\x02\xff",
        bytes(range(256)),
        "🚀".encode(),
    ]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_bytearray_roundtrip() -> None:
    val = bytearray(b"hello world")
    result = await _roundtrip_envelope(val)
    assert result == b"hello world"  # Returns bytes, not bytearray


async def test_envelope_memoryview_roundtrip() -> None:
    val = memoryview(b"memory test")
    result = await _roundtrip_envelope(val)
    assert result == b"memory test"  # Returns bytes, not memoryview


async def test_envelope_tuple_roundtrip() -> None:
    values = [
        (),
        (1,),
        (1, 2, 3),
        ("a", "b", "c"),
        (1, "mixed", math.pi),
        ((1, 2), (3, 4)),  # Nested tuples
    ]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_list_roundtrip() -> None:
    values = [
        [],
        [1],
        [1, 2, 3],
        ["a", "b", "c"],
        [1, "mixed", math.pi],
        [[1, 2], [3, 4]],  # Nested lists
    ]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_dict_roundtrip() -> None:
    values = [
        {},
        {"a": 1},
        {"x": 1, "y": 2, "z": 3},
        {"nested": {"inner": "value"}},
        {"mixed": [1, {"deep": True}]},
    ]
    for val in values:
        assert await _roundtrip_envelope(val) == val


async def test_envelope_deeply_nested_structure() -> None:
    complex_data = {
        "user": {
            "id": uuid.uuid4(),
            "created": datetime.now(timezone.utc),
            "balance": Decimal("1234.56"),
            "metadata": b"binary_data",
            "coordinates": (40.7128, -74.0060),
            "tags": ["premium", "verified"],
            "settings": {
                "notifications": True,
                "theme": "dark",
                "limits": {
                    "daily": Decimal("500.00"),
                    "monthly": Decimal("10000.00"),
                },
            },
        },
        "session": {
            "started": datetime.now(timezone.utc),
            "expires": date.today(),  # noqa: DTZ011
            "token": uuid.uuid4(),
        },
    }
    assert await _roundtrip_envelope(complex_data) == complex_data


async def test_envelope_mixed_type_collections() -> None:
    mixed_list = [
        None,
        True,
        42,
        math.pi,
        "string",
        datetime.now(timezone.utc),
        Decimal("99.99"),
        uuid.uuid4(),
        b"bytes",
        (1, 2, 3),
        [4, 5, 6],
        {"key": "value"},
    ]
    assert await _roundtrip_envelope(mixed_list) == mixed_list


async def test_envelope_tuple_with_all_types() -> None:
    all_types_tuple = (
        None,
        True,
        42,
        math.pi,
        "string",
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        date(2024, 1, 1),
        Decimal("123.45"),
        uuid.uuid4(),
        b"binary",
        [1, 2, 3],
        {"nested": "dict"},
    )
    assert await _roundtrip_envelope(all_types_tuple) == all_types_tuple


@no_type_check
async def test_envelope_unsupported_type_error() -> None:
    serdes = ExtendedTypeSerDes()
    context = SerDesContext("test-op", "test-arn")
    with pytest.raises(SerDesError, match="Unsupported type: <class 'object'>"):
        await serdes.serialize(object())


@no_type_check
async def test_envelope_format_structure() -> None:
    serdes = ExtendedTypeSerDes()
    context = SerDesContext("test-op", "test-arn")
    # Dict will use envelope format, primitives use plain JSON
    serialized = await serdes.serialize({"test": "value"})
    parsed = json.loads(serialized)

    # Verify envelope structure
    assert "t" in parsed
    assert "v" in parsed
    assert parsed["t"] == "m"  # dict tag
    assert parsed["v"]["test"]["v"] == "value"


@no_type_check
async def test_envelope_compact_json_output() -> None:
    serdes = ExtendedTypeSerDes()
    context = SerDesContext("test-op", "test-arn")
    serialized = await serdes.serialize({"key": "value"})
    # Should not contain extra whitespace
    assert " " not in serialized
    assert "\n" not in serialized


@no_type_check
async def test_envelope_bytes_base64_encoding() -> None:
    serdes = ExtendedTypeSerDes()
    context = SerDesContext("test-op", "test-arn")
    test_bytes = b"hello world"
    serialized = await serdes.serialize(test_bytes)
    parsed = json.loads(serialized)

    # Verify base64 encoding
    encoded_value = parsed["v"]
    assert base64.b64decode(encoded_value) == test_bytes


@no_type_check
async def test_envelope_with_main_api() -> None:
    """Test EnvelopeSerDes works with main serialize/deserialize functions."""
    envelope_serdes = ExtendedTypeSerDes()

    test_data = {
        "id": uuid.uuid4(),
        "timestamp": datetime.now(timezone.utc),
        "amount": Decimal("123.45"),
        "data": b"binary_data",
        "coordinates": (40.7128, -74.0060),
        "tags": ["important", "verified"],
    }

    # Serialize with EnvelopeSerDes
    serialized = await serialize(envelope_serdes, test_data, "test-op", "test-arn")

    # Deserialize with EnvelopeSerDes
    deserialized = await deserialize(envelope_serdes, serialized, "test-op", "test-arn")

    assert deserialized == test_data


@no_type_check
async def test_envelope_vs_json_serdes_compatibility() -> None:
    """Test that EnvelopeSerDes and JsonSerDes can coexist."""
    json_serdes = JsonSerDes()
    envelope_serdes = ExtendedTypeSerDes()

    # Simple data that both can handle
    simple_data = {"name": "test", "value": 123, "active": True}

    # Both should serialize successfully
    json_result = await serialize(json_serdes, simple_data, "test-op", "test-arn")
    envelope_result = await serialize(
        envelope_serdes, simple_data, "test-op", "test-arn"
    )

    # Results should be different (envelope has wrapper)
    assert json_result != envelope_result

    # Both should deserialize to same data
    json_deserialized = await deserialize(
        json_serdes, json_result, "test-op", "test-arn"
    )
    envelope_deserialized = await deserialize(
        envelope_serdes, envelope_result, "test-op", "test-arn"
    )

    assert json_deserialized == simple_data
    assert envelope_deserialized == simple_data


@no_type_check
async def test_envelope_handles_json_incompatible_types() -> None:
    """Test that EnvelopeSerDes handles types that JsonSerDes cannot."""
    json_serdes = JsonSerDes()
    envelope_serdes = ExtendedTypeSerDes()

    # Data with types JsonSerDes cannot handle
    complex_data = {
        "uuid": uuid.uuid4(),
        "decimal": Decimal("123.45"),
        "bytes": b"binary",
        "tuple": (1, 2, 3),
    }

    # JsonSerDes should fail
    with pytest.raises(ExecutionError):
        await serialize(json_serdes, complex_data, "test-op", "test-arn")

    # EnvelopeSerDes should succeed
    serialized = await serialize(envelope_serdes, complex_data, "test-op", "test-arn")
    deserialized = await deserialize(envelope_serdes, serialized, "test-op", "test-arn")

    assert deserialized == complex_data


@no_type_check
async def test_envelope_error_handling_with_main_api() -> None:
    """Test error handling when using EnvelopeSerDes with main API."""
    envelope_serdes = ExtendedTypeSerDes()

    # Test serialization error
    with pytest.raises(ExecutionError, match="Serialization failed"):
        await serialize(envelope_serdes, object(), "test-op", "test-arn")

    # Test deserialization error
    with pytest.raises(ExecutionError, match="Deserialization failed"):
        await deserialize(envelope_serdes, "invalid json", "test-op", "test-arn")


async def test_primitive_codec_errors() -> None:
    """Test PrimitiveCodec error cases."""
    primitive_codec = PrimitiveCodec()
    with pytest.raises(SerDesError, match="Unsupported primitive type"):
        primitive_codec.encode(object())

    with pytest.raises(SerDesError, match="Unknown primitive tag"):
        primitive_codec.decode(TypeTag.BYTES, "test")


async def test_bytes_codec_errors() -> None:
    """Test BytesCodec error cases."""
    bytes_codec = BytesCodec()
    with pytest.raises(SerDesError, match="Expected BYTES tag, got"):
        bytes_codec.decode(TypeTag.STR, "test")


async def test_uuid_codec_errors() -> None:
    """Test UuidCodec error cases."""
    uuid_codec = UuidCodec()
    with pytest.raises(SerDesError, match="Expected UUID tag, got"):
        uuid_codec.decode(TypeTag.STR, "test")


async def test_decimal_codec_errors() -> None:
    """Test DecimalCodec error cases."""

    decimal_codec = DecimalCodec()
    with pytest.raises(SerDesError, match="Expected DECIMAL tag, got"):
        decimal_codec.decode(TypeTag.STR, "test")


async def test_datetime_codec_errors() -> None:
    """Test DateTimeCodec error cases."""
    datetime_codec = DateTimeCodec()
    with pytest.raises(SerDesError, match="Unsupported datetime type"):
        datetime_codec.encode("not a datetime")

    with pytest.raises(SerDesError, match="Unknown datetime tag"):
        datetime_codec.decode(TypeTag.BYTES, "test")


async def test_datetime_codec_z_suffix() -> None:
    """Test DateTimeCodec Z suffix handling."""
    datetime_codec = DateTimeCodec()
    result = datetime_codec.decode(TypeTag.DATETIME, "2024-01-01T00:00:00Z")
    expected = datetime.fromisoformat("2024-01-01T00:00:00+00:00")
    assert result == expected


async def test_container_codec_errors() -> None:
    """Test ContainerCodec error cases."""
    container_codec = ContainerCodec()
    type_codec = TypeCodec()
    container_codec.set_dispatcher(type_codec)

    with pytest.raises(SerDesError, match="Unsupported container type"):
        container_codec.encode("not a container")

    with pytest.raises(SerDesError, match="Unknown container tag"):
        container_codec.decode(TypeTag.BYTES, "test")

    with pytest.raises(SerDesError, match="Tuple keys not supported"):
        container_codec.encode({(1, 2): "value"})

    # Test without dispatcher
    container_codec_no_dispatcher = ContainerCodec()
    with pytest.raises(
        DurableExecutionsError,
        match="ContainerCodec not linked to a TypeCodec dispatcher",
    ):
        _ = container_codec_no_dispatcher.dispatcher

    # Test decode with wrong value types
    with pytest.raises(SerDesError, match="Expected list, got"):
        container_codec.decode(TypeTag.LIST, "not a list")

    with pytest.raises(SerDesError, match="Expected list, got"):
        container_codec.decode(TypeTag.TUPLE, "not a list")

    with pytest.raises(SerDesError, match="Expected dict, got"):
        container_codec.decode(TypeTag.DICT, "not a dict")

    # Test _unwrap with plain object (case _ branch)
    result = ContainerCodec._unwrap("plain_string", type_codec)  # noqa: SLF001
    assert result == "plain_string"

    # Test _unwrap with EncodedValue (case EncodedValue branch)
    encoded_val = EncodedValue(TypeTag.STR, "test")
    result = ContainerCodec._unwrap(encoded_val, type_codec)  # noqa: SLF001
    assert result == "test"


@no_type_check
async def test_type_codec_errors() -> None:
    """Test TypeCodec error cases."""
    type_codec = TypeCodec()

    with pytest.raises(SerDesError, match="Unsupported type"):
        type_codec.encode(object())

    class MockTag:
        def __str__(self) -> str:
            return "unknown"

    with pytest.raises(SerDesError, match="Unknown type tag"):
        type_codec.decode(MockTag(), "test")


@no_type_check
async def test_extended_serdes_errors() -> None:
    """Test ExtendedTypesSerDes error cases."""
    serdes = ExtendedTypeSerDes()

    with pytest.raises(
        SerDesError, match='Malformed envelope: missing "t" or "v" at root'
    ):
        await serdes.deserialize('{"invalid": "envelope"}')

    with pytest.raises(SerDesError, match='Unknown type tag: "unknown"'):
        await serdes.deserialize('{"t": "unknown", "v": "test"}')


@no_type_check
async def test_pass_through_serdes() -> None:
    serdes = PassThroughSerDes()

    data = '"name": "test", "value": 123'
    serialized = await serialize(serdes, data, "test-op", "test-arn")
    assert isinstance(serialized, str)
    assert serialized == '"name": "test", "value": 123'
    # Dict uses envelope format, so roundtrip through deserialize
    deserialized = await deserialize(serdes, serialized, "test-op", "test-arn")
    assert deserialized == data


async def test_envelope_large_data_structure() -> None:
    """Test with reasonably large data."""
    large_list = list(range(1000))
    large_dict = {f"key_{i}": f"value_{i}" for i in range(100)}
    large_tuple = tuple(range(500))

    large_structure = {
        "list": large_list,
        "dict": large_dict,
        "tuple": large_tuple,
    }

    result = await _roundtrip_envelope(large_structure)
    assert result == large_structure


async def test_envelope_empty_containers() -> None:
    empty_data = {
        "empty_list": [],
        "empty_dict": {},
        "empty_tuple": (),
        "empty_string": "",
        "empty_bytes": b"",
    }
    assert await _roundtrip_envelope(empty_data) == empty_data


async def test_envelope_type_preservation_after_roundtrip() -> None:
    original = {
        "none": None,
        "bool": True,
        "int": 42,
        "float": math.pi,
        "str": "text",
        "datetime": datetime.now(timezone.utc),
        "date": date.today(),  # noqa: DTZ011
        "decimal": Decimal("123.45"),
        "uuid": uuid.uuid4(),
        "bytes": b"data",
        "tuple": (1, 2, 3),
        "list": [1, 2, 3],
        "dict": {"nested": True},
    }

    result = await _roundtrip_envelope(original)

    # Verify types are preserved
    assert result["none"] is None
    assert type(result["bool"]) is bool
    assert type(result["int"]) is int
    assert type(result["float"]) is float
    assert type(result["str"]) is str
    assert type(result["datetime"]) is datetime
    assert type(result["date"]) is date
    assert type(result["decimal"]) is Decimal
    assert type(result["uuid"]) is uuid.UUID
    assert type(result["bytes"]) is bytes
    assert type(result["tuple"]) is tuple
    assert type(result["list"]) is list
    assert type(result["dict"]) is dict


async def test_envelope_unicode_and_special_characters() -> None:
    unicode_data = {
        "emoji": "🚀🌟💫",
        "chinese": "你好世界",
        "arabic": "مرحبا بالعالم",
        "russian": "Привет мир",
        "special": "\"'\\n\\t\\r",
        "zero_width": "\u200b\u200c\u200d",
    }
    assert await _roundtrip_envelope(unicode_data) == unicode_data


@no_type_check
async def test_primitives() -> None:
    primitives = [
        123,
        "hello",
        True,
        False,
        None,
        math.pi,
        Decimal("10.5"),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        b"bytes here",
        date(2025, 10, 22),
        datetime(2025, 10, 22, 15, 30, 0),  # noqa: DTZ001
    ]
    serdes = ExtendedTypeSerDes()
    for val in primitives:
        serialized = await serdes.serialize(val)
        deserialized = await serdes.deserialize(serialized)
        assert deserialized == val


@no_type_check
async def test_nested_arrays() -> None:
    serdes = ExtendedTypeSerDes()
    val = [1, "two", [3, {"four": 4}], True, b"hi"]
    serialized = await serdes.serialize(val)
    deserialized = await serdes.deserialize(serialized)
    assert deserialized == val


@no_type_check
async def test_nested_dicts() -> None:
    val = {
        "a": 1,
        "b": [2, 3, {"c": 4}],
        "d": {"e": "f", "g": [5, 6]},
        "h": b"bytes in dict",
        "i": uuid.UUID("12345678-1234-5678-1234-567812345678"),
    }
    serdes = ExtendedTypeSerDes()
    serialized = await serdes.serialize(val)
    deserialized = await serdes.deserialize(serialized)
    assert deserialized == val


@no_type_check
async def test_user_dict_with_t_v_keys() -> None:
    val = {"t": "user t value", "v": "user v value"}
    serdes = ExtendedTypeSerDes()
    serialized = await serdes.serialize(val)
    deserialized = await serdes.deserialize(serialized)
    assert deserialized == val


@no_type_check
async def test_complex_nested_structure() -> None:
    val = {
        "list": [1, 2, [3, 4], {"nested_bytes": b"abc"}],
        "tuple": (Decimal("3.14"), True),
        "dict": {
            "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "date": date(2025, 10, 22),
            "datetime": datetime(2025, 10, 22, 15, 30),  # noqa: DTZ001
        },
        "t_v": {"t": "inner t", "v": [1, b"bytes"]},
    }
    serdes = ExtendedTypeSerDes()
    serialized = await serdes.serialize(val)
    deserialized = await serdes.deserialize(serialized)
    assert deserialized == val


@no_type_check
async def test_all_t_v_nested_dicts() -> None:
    val = {
        "t": {"t": "s", "v": "outer t"},
        "v": {
            "t": {"t": "s", "v": "inner t"},
            "v": {"t": {"t": "s", "v": "deep t"}, "v": "deep v"},
        },
    }
    serdes = ExtendedTypeSerDes()
    serialized = await serdes.serialize(val)
    deserialized = await serdes.deserialize(serialized)
    assert deserialized == val

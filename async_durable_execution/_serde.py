"""Value serialization and composable transforms, independent of the journal."""

from __future__ import annotations

import base64
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Generic, Protocol
from uuid import UUID

from ._scope import SerDesContext, active, binding
from ._types import (
    BatchItem,
    BatchResult,
    ErrorObject,
    RetryableSerDesError,
    SerDesError,
    T,
)


class SerDes(ABC, Generic[T]):
    """Asynchronous value codec used by durable operations.

    Implement serialize() to produce a string and deserialize() to restore the
    Python value. Use get_serdes_context() to inspect the current durable owner.
    then() adds reversible string stages without changing the codec instance.
    """

    @abstractmethod
    async def serialize(self, value: T) -> str:
        """Convert a Python value to a string suitable for durable storage.

        Args:
            value: Value produced by the operation.

        Returns:
            (str): Serialized representation to checkpoint.
        """
        ...

    @abstractmethod
    async def deserialize(self, data: str) -> T:
        """Restore a Python value from its persisted string representation.

        Args:
            data: Previously serialized operation value.

        Returns:
            (T): Restored value supplied to workflow code.
        """
        ...

    def then(self, stage):
        """Return a new immutable pipeline containing this value codec and the supplied
        stage.
        """
        return ComposableSerDes(self, (stage,))

    @staticmethod
    def is_primitive(obj):
        """Return whether a value is a JSON scalar or an acyclic list of such values."""
        todo = [(obj, False)]
        ancestors: set[int] = set()
        while todo:
            item, leaving = todo.pop()
            if leaving:
                ancestors.remove(id(item))
            elif item is None or isinstance(item, (str, int, float, bool)):
                continue
            elif not isinstance(item, list) or id(item) in ancestors:
                return False
            else:
                ancestors.add(id(item))
                todo.append((item, True))
                todo.extend((child, False) for child in reversed(item))
        return True


class JsonSerDes(SerDes[T]):
    """Encode and decode JSON-compatible values using Python's json module.

    This codec follows standard JSON conversions, including tuples becoming lists
    and JSON object keys being strings.
    """

    async def serialize(self, value: T) -> str:
        """Encode a JSON-compatible value as a string with json.dumps()."""
        return json.dumps(value)

    async def deserialize(self, data: str) -> T:
        """Decode a JSON string into ordinary Python values with json.loads()."""
        return json.loads(data)


@dataclass(frozen=True)
class Encoded:
    tag: str
    value: Any


class ExtendedTypeSerDes(SerDes[T]):
    """Tagged JSON preserving Python values and extension-owned types.

    This version's payload format is independent of previous SDK internals.
    Plain JSON values are also accepted for callback and invocation results.
    """

    def __init__(self, type_codecs=()):
        """Configure optional codecs for additional Python value types.

        Built-in values include JSON scalars, dictionaries, lists, tuples, bytes,
        UUIDs, decimals, dates, and datetimes, plus SDK result types.

        Args:
            type_codecs (tuple): Extension objects exposing tag, can_encode(), encode(),
                and decode(). Recursive encode/decode callbacks are supplied by the
                codec.

        Raises:
            ValueError: Extension tags repeat or conflict with built-in tags.
        """
        self._extensions = tuple(type_codecs)
        tags = [item.tag for item in self._extensions]
        if len(set(tags)) != len(tags) or any(
            t
            in {
                "bytes",
                "uuid",
                "decimal",
                "datetime",
                "date",
                "tuple",
                "list",
                "dict",
                "error",
                "batch",
                "plain",
            }
            for t in tags
        ):
            raise ValueError(
                "Codec tags must be unique and must not replace built-in tags"
            )

    def _pack(self, value, ancestors):
        if id(value) in ancestors:
            raise SerDesError("Circular values cannot be serialized")
        path = ancestors | {id(value)}
        for codec in self._extensions:
            if codec.can_encode(value):
                return [
                    "extension",
                    codec.tag,
                    codec.encode(value, lambda v: Encoded("ade3", self._pack(v, path))),
                ]
        if value is None or isinstance(value, (bool, int, float, str)):
            return ["plain", value]
        if isinstance(value, (bytes, bytearray, memoryview)):
            return ["bytes", base64.b64encode(value).decode("ascii")]
        if isinstance(value, UUID):
            return ["uuid", str(value)]
        if isinstance(value, Decimal):
            return ["decimal", str(value)]
        if isinstance(value, datetime):
            return ["datetime", value.isoformat()]
        if isinstance(value, date):
            return ["date", value.isoformat()]
        if isinstance(value, ErrorObject):
            return ["error", value.to_dict()]
        if isinstance(value, BatchResult):
            return ["batch", self._pack(value.to_dict(), path)]
        if isinstance(value, dict):
            return [
                "dict",
                [[self._pack(k, path), self._pack(v, path)] for k, v in value.items()],
            ]
        if isinstance(value, (tuple, list)):
            return [
                "tuple" if isinstance(value, tuple) else "list",
                [self._pack(v, path) for v in value],
            ]
        # Graph-specific values are owned by the graph module, registered explicitly.
        registration = _value_types.get(type(value))
        if registration:
            name, encode, _ = registration
            return ["registered", name, self._pack(encode(value), path)]
        raise SerDesError(f"Unsupported serialized value: {type(value).__name__}")

    def _unpack(self, value):
        if not isinstance(value, list) or not value:
            raise SerDesError("Invalid typed value")
        tag, payload = value[0], value[1]
        decoders = {
            "plain": lambda x: x,
            "bytes": lambda x: base64.b64decode(x, validate=True),
            "uuid": UUID,
            "decimal": Decimal,
            "datetime": datetime.fromisoformat,
            "date": date.fromisoformat,
            "error": ErrorObject.from_dict,
        }
        if tag in decoders:
            return decoders[tag](payload)
        if tag == "dict":
            return {self._unpack(k): self._unpack(v) for k, v in payload}
        if tag in ("list", "tuple"):
            items = [self._unpack(v) for v in payload]
            return tuple(items) if tag == "tuple" else items
        if tag == "batch":
            return BatchResult.from_dict(self._unpack(payload))
        if tag == "extension":
            for codec in self._extensions:
                if codec.tag == payload:
                    return codec.decode(
                        value[2],
                        lambda t, v: self._unpack(v)
                        if t == "ade3"
                        else self._unpack([t, v]),
                    )
        if tag == "registered":
            for name, _, decode in _value_types.values():
                if name == payload:
                    return decode(self._unpack(value[2]))
        raise SerDesError(f"Unknown serialized type: {tag}")

    def serialize_sync(self, value):
        """Encode a supported Python value as a versioned typed JSON string.

        Raises:
            SerDesError: The value contains unsupported types or circular references.
        """
        try:
            encoded = self._pack(value, set())
            return json.dumps(
                {"ade": 3, "value": encoded},
                ensure_ascii=False,
                separators=(",", ":"),
                default=lambda v: {"t": v.tag, "v": v.value}
                if isinstance(v, Encoded)
                else _unsupported(v),
            )
        except (TypeError, ValueError, RecursionError) as error:
            raise SerDesError(f"Cannot serialize value: {error}") from error

    def deserialize_sync(self, data):
        """Decode typed JSON values, accepting ordinary JSON as a pass-through format.

        Raises:
            SerDesError: JSON is malformed or a typed representation cannot be decoded.
        """
        try:
            decoded = json.loads(data)
            if (
                isinstance(decoded, dict)
                and decoded.get("ade") == 3
                and set(decoded) == {"ade", "value"}
            ):
                return self._unpack(decoded["value"])
            return decoded
        except (TypeError, ValueError, KeyError, IndexError, RecursionError) as error:
            raise SerDesError(f"Cannot deserialize value: {error}") from error

    async def serialize(self, value):
        """Encode a supported value using the asynchronous SerDes interface."""
        return self.serialize_sync(value)

    async def deserialize(self, data):
        """Decode typed or ordinary JSON through the asynchronous SerDes interface."""
        return self.deserialize_sync(data)


def _unsupported(value):
    raise TypeError(f"Unsupported codec value: {type(value).__name__}")


_value_types: dict[type, tuple[str, Any, Any]] = {}


def register_value(kind, name, encode, decode):
    _value_types[kind] = (name, encode, decode)


class SerDesStage(Protocol):
    """Reversible string transformation applied after a value codec.

    A stage must recognize its own envelopes, reject malformed recognized input,
    and pass unrecognized input through unchanged. Context identifies the durable
    owner and carries the original value during serialization.
    """

    async def serialize(self, value: str, context: SerDesContext) -> str:
        """Transform the preceding codec or stage's string before checkpointing.

        Args:
            value: String produced by the preceding component.
            context: Durable owner metadata; original_value is the original Python
                object.

        Returns:
            (str): Transformed representation for the next stage or durable storage.
        """
        ...

    async def deserialize(self, data: str, context: SerDesContext) -> str:
        """Reverse a recognized envelope or return unrecognized strings unchanged.

        Args:
            data: Persisted string produced by this stage.
            context: Durable owner metadata; original_value is None on deserialization.

        Returns:
            (str): String for the preceding stage or root value codec to deserialize.
        """
        ...


class SerDesPipelineError(SerDesError):
    """Permanent pipeline failure identifying the responsible component.

    Attributes:
        stage_index (int): Zero for the value codec; one-based index for string stages.
        action (str): serialize or deserialize.
        stage (object): Component that raised or returned an invalid value.
    """

    def __init__(self, stage_index, action, stage):
        """Identify the failing pipeline component, its index, and conversion direction."""
        self.stage_index, self.action, self.stage = stage_index, action, stage
        super().__init__(
            f"SerDes pipeline stage {stage_index} ({type(stage).__name__}) failed to {action}"
        )


class ComposableSerDes(SerDes[T]):
    """A value codec followed by an immutable sequence of reversible string stages.

    Serialization runs forward; deserialization runs stages in reverse before
    decoding the value. Permanent component failures become SerDesPipelineError;
    RetryableSerDesError propagates with its retry semantics intact.
    """

    def __init__(self, value_codec, stages=()):
        """Compose a value codec and ordered string stages.

        Args:
            value_codec (SerDes): Root codec, or another pipeline whose stages are
                flattened.
            stages (tuple[SerDesStage, ...]): Additional transformations in
                serialization order.

        Raises:
            TypeError: The value codec or any stage is None.
        """
        if value_codec is None or any(item is None for item in stages):
            raise TypeError("Pipeline components cannot be None")
        if isinstance(value_codec, ComposableSerDes):
            self._codec, self._stages = (
                value_codec.value_codec,
                value_codec.stages + tuple(stages),
            )
        else:
            self._codec, self._stages = value_codec, tuple(stages)

    @property
    def value_codec(self):
        """Return the root value codec, excluding string-transform stages."""
        return self._codec

    @property
    def stages(self):
        """Return the immutable tuple of string stages in serialization order."""
        return self._stages

    def then(self, stage):
        """Return a new pipeline with one stage appended, leaving this pipeline unchanged."""
        return ComposableSerDes(self, (stage,))

    async def _transform(self, action, value):
        ambient = active.get()
        context = ambient if isinstance(ambient, SerDesContext) else SerDesContext()
        context = replace(
            context, original_value=value if action == "serialize" else None
        )
        components = (self.value_codec, *self.stages)
        order = (
            range(len(components))
            if action == "serialize"
            else range(len(components) - 1, -1, -1)
        )
        for index in order:
            component = components[index]
            try:
                value = (
                    await getattr(component, action)(value, context)
                    if index
                    else await getattr(component, action)(value)
                )
                if (index or action == "serialize") and not isinstance(value, str):
                    raise TypeError("Serialized pipeline values must be strings")
            except RetryableSerDesError:
                raise
            except Exception as error:
                raise SerDesPipelineError(index, action, component) from error
        return value

    async def serialize(self, value):
        """Encode a value and run stages forward with its original value in context.

        Args:
            value (T): Python value to encode.

        Returns:
            (str): Final string produced by the pipeline.
        """
        return await self._transform("serialize", value)

    async def deserialize(self, data):
        """Reverse the string stages, then decode the restored root representation.

        Args:
            data (str): Final persisted pipeline string.

        Returns:
            (T): Value restored by the root codec.
        """
        return await self._transform("deserialize", data)


def create_serdes_pipeline(value_codec, *stages):
    """Compose a value codec with zero or more reversible string stages.

    Args:
        value_codec (SerDes): Root Python-value codec.
        *stages (SerDesStage): Transformations in serialization order.

    Returns:
        (ComposableSerDes): A pipeline whose stages reverse order on deserialization.
    """
    return ComposableSerDes(value_codec, stages)


def is_composable_serdes(serdes):
    """Return whether a codec is a ComposableSerDes pipeline."""
    return isinstance(serdes, ComposableSerDes)


default_codec: ExtendedTypeSerDes[Any] = ExtendedTypeSerDes()


async def convert(codec, action, value, context):
    with binding(context):
        try:
            output = await getattr(
                codec if codec is not None else default_codec, action
            )(value)
            if action == "serialize" and not isinstance(output, str):
                raise SerDesError("Serializers must return a string")
            return output
        except RetryableSerDesError:
            raise
        except SerDesError:
            raise
        except Exception as error:
            raise SerDesError(
                f"{action} failed for {context.entity_id}: {error}"
            ) from error

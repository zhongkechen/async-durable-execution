"""Declarative acyclic durable workflow composition."""

from __future__ import annotations

import asyncio
import builtins
import functools
import heapq
import inspect
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping, Set
from contextvars import ContextVar
from dataclasses import dataclass, field, fields as dataclass_fields, is_dataclass
from enum import Enum
from typing import Any, Generic, NoReturn, ParamSpec, TypeVar, cast

from .._core import (
    CallableRuntimeError,
    DurableContext,
    DurableExecutionsError,
    ErrorObject,
    ExecutionError,
    ExtendedTypeSerDes,
    InvalidStateError,
    InvocationError,
    SerDes,
    SerDesError,
    SuspendExecution,
    TimedSuspendExecution,
    ValidationError,
    MappingModel,
    _restore_sdk_control_error,
    bind_current_context,
    bind_durable_definition,
    create_eager_task,
    ensure_durable_operations_allowed,
    get_current_context,
    get_durable_context,
)
from .._primitive.child import run_in_child_context
from .parallel import _BatchResultSerDes


T = TypeVar("T")
Params = ParamSpec("Params")
_BASE_EXCEPTION_GROUP_TYPE = getattr(builtins, "BaseExceptionGroup", None)


class FlowDefinitionError(ValidationError):
    """Raised when a declarative flow definition is invalid."""


class FlowExecutionError(DurableExecutionsError):
    """Raised after a flow checkpoints a result with unhandled node failures."""

    def __init__(self, message: str, result: Any) -> None:
        super().__init__(message)
        self.result = result


class FlowNodeStatus(Enum):
    """Logical status of a node in a completed flow."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class FlowNodeResult(MappingModel, Generic[T]):
    """Logical result of one flow node."""

    status: FlowNodeStatus
    outcome: T | None = field(default=None, metadata={"omit_if_none": False})
    error: ErrorObject | None = field(default=None, metadata={"omit_if_none": False})

    @classmethod
    def succeeded(cls, outcome: T) -> FlowNodeResult[T]:
        return cls(status=FlowNodeStatus.SUCCEEDED, outcome=outcome)

    @classmethod
    def failed(cls, error: ErrorObject) -> FlowNodeResult[T]:
        return cls(status=FlowNodeStatus.FAILED, error=error)

    @classmethod
    def skipped(cls) -> FlowNodeResult[T]:
        return cls(status=FlowNodeStatus.SKIPPED)


@dataclass(frozen=True)
class FlowResult:
    """Complete logical result of a flow."""

    results: dict[str, FlowNodeResult[Any]]
    outputs: tuple[Any, ...] = ()
    unhandled_failures: tuple[str, ...] = ()
    unavailable_outputs: tuple[str, ...] = ()
    _output_kinds: tuple[_FlowNodeInputKind, ...] = field(
        default=(),
        repr=False,
    )

    def __post_init__(self) -> None:
        if not self._output_kinds and self.outputs:
            object.__setattr__(
                self,
                "_output_kinds",
                (_FlowNodeInputKind.RESULT,) * len(self.outputs),
            )
        if len(self._output_kinds) != len(self.outputs):
            msg = "Flow output values and projections must have the same length."
            raise InvalidStateError(msg)

    @property
    def output(self) -> Any:
        """Return selected output while preserving the definition's arity."""
        if not self.outputs:
            return None
        if len(self.outputs) == 1:
            return self.outputs[0]
        return self.outputs

    @property
    def has_unhandled_failures(self) -> bool:
        return bool(self.unhandled_failures)

    @property
    def has_unavailable_outputs(self) -> bool:
        return bool(self.unavailable_outputs)

    def get_result(self, name: str) -> FlowNodeResult[Any]:
        """Return a node result by its declared name."""
        try:
            return self.results[name]
        except KeyError:
            msg = f"Flow has no node named {name!r}."
            raise KeyError(msg) from None

    def to_dict(self) -> dict[str, Any]:
        """Convert the flow result to a serialization-friendly mapping."""
        return {
            "results": {
                name: node_result.to_dict()
                for name, node_result in self.results.items()
            },
            "outputs": [
                _flow_output_to_dict(output, kind)
                for output, kind in zip(
                    self.outputs,
                    self._output_kinds,
                    strict=True,
                )
            ],
            "outputProjections": [kind.value for kind in self._output_kinds],
            "unhandledFailures": list(self.unhandled_failures),
            "unavailableOutputs": list(self.unavailable_outputs),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FlowResult:
        raw_outputs = tuple(data.get("outputs", ()))
        raw_kinds = data.get("outputProjections")
        try:
            output_kinds = (
                tuple(_FlowNodeInputKind(str(kind)) for kind in raw_kinds)
                if raw_kinds is not None
                else (_FlowNodeInputKind.RESULT,) * len(raw_outputs)
            )
        except (TypeError, ValueError) as error:
            msg = "Serialized flow result contains an invalid output projection."
            raise SerDesError(msg) from error
        if len(output_kinds) != len(raw_outputs):
            msg = (
                "Serialized flow output values and projections have different lengths."
            )
            raise SerDesError(msg)
        return cls(
            results={
                str(name): FlowNodeResult.from_dict(node_result)
                for name, node_result in data["results"].items()
            },
            outputs=tuple(
                _flow_output_from_dict(output, kind)
                for output, kind in zip(
                    raw_outputs,
                    output_kinds,
                    strict=True,
                )
            ),
            unhandled_failures=tuple(
                str(name) for name in data.get("unhandledFailures", ())
            ),
            unavailable_outputs=tuple(
                str(name) for name in data.get("unavailableOutputs", ())
            ),
            _output_kinds=output_kinds,
        )


class _DependencyCondition(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"

    def matches(self, status: FlowNodeStatus) -> bool:
        if self is _DependencyCondition.COMPLETED:
            return True
        if self is _DependencyCondition.SUCCEEDED:
            return status is FlowNodeStatus.SUCCEEDED
        return status is FlowNodeStatus.FAILED


class _DependencyMode(Enum):
    ALL = "ALL"
    ANY = "ANY"


class _FlowNodeInputKind(Enum):
    OUTCOME = "OUTCOME"
    ERROR = "ERROR"
    RESULT = "RESULT"


def _flow_output_to_dict(value: Any, kind: _FlowNodeInputKind) -> Any:
    if kind is _FlowNodeInputKind.ERROR:
        return value.to_dict() if value is not None else None
    if kind is _FlowNodeInputKind.RESULT:
        return cast("FlowNodeResult[Any]", value).to_dict()
    return value


def _flow_output_from_dict(value: Any, kind: _FlowNodeInputKind) -> Any:
    if kind is _FlowNodeInputKind.ERROR:
        return ErrorObject.from_dict(value) if value is not None else None
    if kind is _FlowNodeInputKind.RESULT:
        return FlowNodeResult.from_dict(value)
    return value


def _flow_node_result_to_checkpoint_dict(
    value: FlowNodeResult[Any],
) -> dict[str, Any]:
    """Preserve outcomes for type-aware encoding by ExtendedTypeSerDes."""
    return {
        "status": value.status.value,
        "outcome": value.outcome,
        "error": value.error.to_dict() if value.error is not None else None,
    }


def _flow_result_to_checkpoint_dict(value: FlowResult) -> dict[str, Any]:
    outputs = [
        (
            _flow_node_result_to_checkpoint_dict(cast("FlowNodeResult[Any]", output))
            if kind is _FlowNodeInputKind.RESULT
            else _flow_output_to_dict(output, kind)
        )
        for output, kind in zip(
            value.outputs,
            value._output_kinds,
            strict=True,
        )
    ]
    return {
        "results": {
            name: _flow_node_result_to_checkpoint_dict(node_result)
            for name, node_result in value.results.items()
        },
        "outputs": outputs,
        "outputProjections": [kind.value for kind in value._output_kinds],
        "unhandledFailures": list(value.unhandled_failures),
        "unavailableOutputs": list(value.unavailable_outputs),
    }


_FLOW_VALUE_VERSION_KEY = "__async_durable_execution_flow_value__"
_FLOW_VALUE_VERSION = 1
_FLOW_VALUE_KIND_KEY = "kind"
_FLOW_VALUE_PAYLOAD_KEY = "value"


class _FlowValueKind(Enum):
    ESCAPED_DICT = "ESCAPED_DICT"
    ERROR_OBJECT = "ERROR_OBJECT"
    FLOW_NODE_RESULT = "FLOW_NODE_RESULT"
    FLOW_RESULT = "FLOW_RESULT"


def _encode_flow_value(
    value: Any,
    *,
    active_objects: set[int] | None = None,
) -> Any:
    recursive = isinstance(
        value,
        (FlowResult, FlowNodeResult, ErrorObject, list, tuple, dict),
    )
    if active_objects is None:
        active_objects = set()
    object_id = id(value)
    if recursive and object_id in active_objects:
        msg = "Circular references are not supported in flow values."
        raise SerDesError(msg)
    if recursive:
        active_objects.add(object_id)

    try:
        if isinstance(value, FlowResult):
            kind = _FlowValueKind.FLOW_RESULT
            payload = _encode_flow_value(
                _flow_result_to_checkpoint_dict(value),
                active_objects=active_objects,
            )
        elif isinstance(value, FlowNodeResult):
            kind = _FlowValueKind.FLOW_NODE_RESULT
            payload = _encode_flow_value(
                _flow_node_result_to_checkpoint_dict(value),
                active_objects=active_objects,
            )
        elif isinstance(value, ErrorObject):
            kind = _FlowValueKind.ERROR_OBJECT
            payload = _encode_flow_value(
                value.to_dict(),
                active_objects=active_objects,
            )
        elif isinstance(value, list):
            return [
                _encode_flow_value(item, active_objects=active_objects)
                for item in value
            ]
        elif isinstance(value, tuple):
            return tuple(
                _encode_flow_value(item, active_objects=active_objects)
                for item in value
            )
        elif isinstance(value, dict):
            payload = {
                key: _encode_flow_value(item, active_objects=active_objects)
                for key, item in value.items()
            }
            if _FLOW_VALUE_VERSION_KEY not in value:
                return payload
            kind = _FlowValueKind.ESCAPED_DICT
        else:
            return value
        return {
            _FLOW_VALUE_VERSION_KEY: _FLOW_VALUE_VERSION,
            _FLOW_VALUE_KIND_KEY: kind.value,
            _FLOW_VALUE_PAYLOAD_KEY: payload,
        }
    finally:
        if recursive:
            active_objects.remove(object_id)


def _decode_flow_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_flow_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_decode_flow_value(item) for item in value)
    if not isinstance(value, Mapping):
        return value
    if _FLOW_VALUE_VERSION_KEY not in value:
        return {key: _decode_flow_value(item) for key, item in value.items()}
    if (
        type(value.get(_FLOW_VALUE_VERSION_KEY)) is not int
        or value[_FLOW_VALUE_VERSION_KEY] != _FLOW_VALUE_VERSION
    ):
        msg = "Serialized flow value has an invalid envelope."
        raise SerDesError(msg)
    try:
        kind = _FlowValueKind(value[_FLOW_VALUE_KIND_KEY])
        payload = value[_FLOW_VALUE_PAYLOAD_KEY]
    except (KeyError, TypeError, ValueError) as error:
        msg = "Serialized flow value has an invalid kind or payload."
        raise SerDesError(msg) from error

    if kind is _FlowValueKind.ESCAPED_DICT:
        if not isinstance(payload, Mapping):
            msg = "Serialized escaped dict flow value must be a mapping."
            raise SerDesError(msg)
        return {key: _decode_flow_value(item) for key, item in payload.items()}

    decoded = _decode_flow_value(payload)
    if not isinstance(decoded, Mapping):
        msg = f"Serialized {kind.value.lower()} flow value must contain a mapping."
        raise SerDesError(msg)
    if kind is _FlowValueKind.ERROR_OBJECT:
        return ErrorObject.from_dict(decoded)
    if kind is _FlowValueKind.FLOW_NODE_RESULT:
        return FlowNodeResult.from_dict(decoded)
    return FlowResult.from_dict(decoded)


class _EvaluationStatus(Enum):
    PENDING = "PENDING"
    MATCHED = "MATCHED"
    UNMATCHED = "UNMATCHED"


@dataclass(frozen=True)
class _Evaluation:
    status: _EvaluationStatus
    handled_failures: tuple[FlowNode[Any], ...] = ()


class _DependencyExpression:
    """Internal immutable dependency expression."""

    builder: _FlowBuilder

    def leaves(self) -> tuple[_DependencyLeaf, ...]:
        raise NotImplementedError

    def evaluate(
        self,
        results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
        any_winners: dict[int, int] | None = None,
    ) -> _Evaluation:
        raise NotImplementedError

    def __and__(
        self, other: FlowNode[Any] | _DependencyExpression
    ) -> _DependencyExpression:
        return self._combine(other, _DependencyMode.ALL)

    def __or__(
        self, other: FlowNode[Any] | _DependencyExpression
    ) -> _DependencyExpression:
        return self._combine(other, _DependencyMode.ANY)

    def _combine(
        self,
        other: FlowNode[Any] | _DependencyExpression,
        mode: _DependencyMode,
    ) -> _DependencyExpression:
        _require_active_builder(self.builder)
        other_expression = _coerce_expression(other)
        if other_expression.builder is not self.builder:
            msg = "Dependency expressions from different flow definitions cannot be mixed."
            raise InvalidStateError(msg)

        children: list[_DependencyExpression] = []
        for expression in (self, other_expression):
            if (
                isinstance(expression, _CompositeDependencyExpression)
                and expression.mode is mode
            ):
                children.extend(expression.children)
            else:
                children.append(expression)
        return _CompositeDependencyExpression(
            builder=self.builder,
            mode=mode,
            children=tuple(children),
        )

    def __rshift__(
        self, target: FlowNode[Any] | tuple[FlowNode[Any], ...]
    ) -> FlowNode[Any] | tuple[FlowNode[Any], ...]:
        _require_active_builder(self.builder)
        targets = target if isinstance(target, tuple) else (target,)
        if not targets:
            msg = "A dependency expression must target at least one node."
            raise FlowDefinitionError(msg)

        for flow_node in targets:
            if not isinstance(flow_node, FlowNode):
                msg = "Dependency targets must be FlowNode instances."
                raise FlowDefinitionError(msg)
            if flow_node._builder is not self.builder:
                msg = "Nodes from different flow definitions cannot be mixed."
                raise InvalidStateError(msg)
            self.builder.add_dependency(flow_node, self)
        return target


@dataclass(frozen=True)
class _FlowNodeInput(Generic[T]):
    """Deferred node projection used while a flow definition is evaluated."""

    node: FlowNode[Any]
    kind: _FlowNodeInputKind

    @property
    def condition(self) -> _DependencyCondition:
        if self.kind is _FlowNodeInputKind.OUTCOME:
            return _DependencyCondition.SUCCEEDED
        if self.kind is _FlowNodeInputKind.ERROR:
            return _DependencyCondition.FAILED
        return _DependencyCondition.COMPLETED

    def resolve(
        self,
        results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
    ) -> T:
        result = results.get(self.node)
        if result is None:
            msg = f"Result for required input {self.node.name!r} is not available."
            raise InvalidStateError(msg)
        if not self.condition.matches(result.status):
            msg = (
                f"Result for required input {self.node.name!r} has status "
                f"{result.status.value}, expected {self.condition.value}."
            )
            raise InvalidStateError(msg)
        if self.kind is _FlowNodeInputKind.OUTCOME:
            return cast("T", result.outcome)
        if self.kind is _FlowNodeInputKind.ERROR and result.error is None:
            msg = f"Failed input {self.node.name!r} has no error."
            raise InvalidStateError(msg)
        if self.kind is _FlowNodeInputKind.ERROR:
            return cast("T", result.error)
        return cast("T", result)

    def project(self, result: FlowNodeResult[Any]) -> T:
        """Project a settled node result without imposing an input condition."""
        if self.kind is _FlowNodeInputKind.OUTCOME:
            return cast("T", result.outcome)
        if self.kind is _FlowNodeInputKind.ERROR:
            return cast("T", result.error)
        return cast("T", result)


@dataclass(frozen=True)
class _DependencyLeaf(_DependencyExpression):
    builder: _FlowBuilder
    node: FlowNode[Any]
    condition: _DependencyCondition

    def leaves(self) -> tuple[_DependencyLeaf, ...]:
        return (self,)

    def evaluate(
        self,
        results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
        any_winners: dict[int, int] | None = None,
    ) -> _Evaluation:
        node_result = results.get(self.node)
        if node_result is None:
            return _Evaluation(_EvaluationStatus.PENDING)
        if not self.condition.matches(node_result.status):
            return _Evaluation(_EvaluationStatus.UNMATCHED)

        handled = (
            (self.node,)
            if self.condition is _DependencyCondition.FAILED
            and node_result.status is FlowNodeStatus.FAILED
            else ()
        )
        return _Evaluation(_EvaluationStatus.MATCHED, handled)


@dataclass(frozen=True)
class _CompositeDependencyExpression(_DependencyExpression):
    builder: _FlowBuilder
    mode: _DependencyMode
    children: tuple[_DependencyExpression, ...]

    def leaves(self) -> tuple[_DependencyLeaf, ...]:
        return tuple(leaf for child in self.children for leaf in child.leaves())

    def evaluate(
        self,
        results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
        any_winners: dict[int, int] | None = None,
    ) -> _Evaluation:
        if any_winners is None:
            any_winners = {}
        if self.mode is _DependencyMode.ANY and id(self) in any_winners:
            winner = any_winners[id(self)]
            return self.children[winner].evaluate(results, any_winners)

        evaluations = [child.evaluate(results, any_winners) for child in self.children]
        if self.mode is _DependencyMode.ALL:
            if any(
                evaluation.status is _EvaluationStatus.PENDING
                for evaluation in evaluations
            ):
                return _Evaluation(_EvaluationStatus.PENDING)
            if any(
                evaluation.status is _EvaluationStatus.UNMATCHED
                for evaluation in evaluations
            ):
                return _Evaluation(_EvaluationStatus.UNMATCHED)
            return _Evaluation(
                _EvaluationStatus.MATCHED,
                tuple(
                    node
                    for evaluation in evaluations
                    for node in evaluation.handled_failures
                ),
            )

        for index, evaluation in enumerate(evaluations):
            if evaluation.status is _EvaluationStatus.MATCHED:
                any_winners[id(self)] = index
                return evaluation
        if any(
            evaluation.status is _EvaluationStatus.PENDING for evaluation in evaluations
        ):
            return _Evaluation(_EvaluationStatus.PENDING)
        return _Evaluation(_EvaluationStatus.UNMATCHED)


class FlowNode(Generic[T]):
    """Typed handle for a node declared inside a durable DAG definition."""

    def __init__(
        self,
        builder: _FlowBuilder,
        index: int,
        func: Callable[[], Awaitable[T]],
        name: str,
    ) -> None:
        self._builder = builder
        self._index = index
        self._func = func
        self.name = name
        self._dependency: _DependencyExpression | None = None

    def __repr__(self) -> str:
        return f"FlowNode(name={self.name!r})"

    @property
    def result(self) -> FlowNodeResult[T]:
        """Reference a completed output or return its result in a running node."""
        builder = _current_flow_builder.get()
        if builder is not None and not builder.frozen:
            return cast(
                "FlowNodeResult[T]",
                _FlowNodeInput[FlowNodeResult[T]](
                    self,
                    _FlowNodeInputKind.RESULT,
                ),
            )
        try:
            context = get_current_context()
        except RuntimeError as error:
            msg = "Flow node results are only available while a flow node is executing."
            raise InvalidStateError(msg) from error
        if not isinstance(context, FlowNodeContext):
            msg = "Flow node results are only available while a flow node is executing."
            raise InvalidStateError(msg)
        return context.result(self)

    @property
    def status(self) -> FlowNodeStatus:
        """Return this direct dependency's logical status."""
        return self.result.status

    @property
    def outcome(self) -> T:
        """Reference a required successful input or return its resolved outcome."""
        builder = _current_flow_builder.get()
        if builder is not None and not builder.frozen:
            return cast(
                "T",
                _FlowNodeInput[Any](self, _FlowNodeInputKind.OUTCOME),
            )
        result = self.result
        if result.status is not FlowNodeStatus.SUCCEEDED:
            msg = (
                f"Flow node {self.name!r} did not succeed "
                f"(status {result.status.value}); inspect status, error, or result."
            )
            raise InvalidStateError(msg)
        return cast("T", result.outcome)

    @property
    def error(self) -> ErrorObject | None:
        """Reference a required failed input or return its captured error."""
        builder = _current_flow_builder.get()
        if builder is not None and not builder.frozen:
            return cast(
                "ErrorObject",
                _FlowNodeInput[ErrorObject](self, _FlowNodeInputKind.ERROR),
            )
        return self.result.error

    @property
    def succeeded(self) -> _DependencyExpression:
        return _DependencyLeaf(self._builder, self, _DependencyCondition.SUCCEEDED)

    @property
    def failed(self) -> _DependencyExpression:
        return _DependencyLeaf(self._builder, self, _DependencyCondition.FAILED)

    @property
    def completed(self) -> _DependencyExpression:
        return _DependencyLeaf(self._builder, self, _DependencyCondition.COMPLETED)

    def __and__(
        self, other: FlowNode[Any] | _DependencyExpression
    ) -> _DependencyExpression:
        return self.succeeded & other

    def __or__(
        self, other: FlowNode[Any] | _DependencyExpression
    ) -> _DependencyExpression:
        return self.succeeded | other

    def __rshift__(
        self, target: FlowNode[Any] | tuple[FlowNode[Any], ...]
    ) -> FlowNode[Any] | tuple[FlowNode[Any], ...]:
        return self.succeeded >> target


@dataclass(frozen=True)
class FlowNodeContext(DurableContext):
    """Durable context exposed by get_node_context() inside a flow node."""

    _direct_dependencies: frozenset[FlowNode[Any]] = field(default_factory=frozenset)
    _dependency_results: Mapping[FlowNode[Any], FlowNodeResult[Any]] = field(
        default_factory=dict
    )

    def result(self, dependency: FlowNode[T]) -> FlowNodeResult[T]:
        """Return the settled result of a declared direct dependency."""
        if dependency not in self._direct_dependencies:
            msg = (
                f"Node {dependency.name!r} is not a direct dependency of the "
                "current flow node."
            )
            raise InvalidStateError(msg)
        if dependency not in self._dependency_results:
            msg = (
                f"Result for dependency {dependency.name!r} is not available. "
                "An ANY dependency may start before its other branches settle."
            )
            raise InvalidStateError(msg)
        return cast("FlowNodeResult[T]", self._dependency_results[dependency])

    @property
    def dependency_results(self) -> Mapping[str, FlowNodeResult[Any]]:
        """Return settled direct dependency results captured for this node."""
        return {
            dependency.name: result
            for dependency, result in self._dependency_results.items()
        }

    def get_dependency_result(self, name: str) -> FlowNodeResult[Any] | None:
        """Return an available direct dependency result by stable node name."""
        dependency = self._dependency_by_name(name)
        return self._dependency_results.get(dependency)

    def require_dependency_result(self, name: str) -> FlowNodeResult[Any]:
        """Return an available direct dependency result or raise."""
        result = self.get_dependency_result(name)
        if result is None:
            msg = (
                f"Result for dependency {name!r} is not available. "
                "An ANY dependency may start before its other branches settle."
            )
            raise InvalidStateError(msg)
        return result

    def _dependency_by_name(self, name: str) -> FlowNode[Any]:
        for dependency in self._direct_dependencies:
            if dependency.name == name:
                return dependency
        msg = f"Node {name!r} is not a direct dependency of the current flow node."
        raise InvalidStateError(msg)


def get_node_context() -> FlowNodeContext:
    """Return the active `FlowNodeContext`."""
    current_context = get_current_context()
    if not isinstance(current_context, FlowNodeContext):
        msg = "get_node_context() can only be used while a flow node is executing."
        raise RuntimeError(msg)
    return current_context


@dataclass(frozen=True)
class _FrozenFlow:
    nodes: tuple[FlowNode[Any], ...]
    topological_nodes: tuple[FlowNode[Any], ...]
    execution_nodes: tuple[FlowNode[Any], ...]
    outputs: tuple[_FlowNodeInput[Any], ...]


class _FlowBuilder:
    def __init__(self) -> None:
        self.nodes: list[FlowNode[Any]] = []
        self.frozen = False

    def add_node(
        self,
        func: Callable[[], Awaitable[T]],
        name: str,
    ) -> FlowNode[T]:
        if self.frozen:
            msg = "Cannot add nodes after a flow definition has been frozen."
            raise InvalidStateError(msg)
        flow_node = FlowNode(self, len(self.nodes), func, name)
        self.nodes.append(flow_node)
        return flow_node

    def add_dependency(
        self, target: FlowNode[Any], expression: _DependencyExpression
    ) -> None:
        if self.frozen:
            msg = "Cannot add dependencies after a flow definition has been frozen."
            raise InvalidStateError(msg)
        if target._dependency is not None:
            msg = (
                f"Node {target.name!r} already has a dependency expression. "
                "Use an explicit '&' or '|' expression for multiple dependencies."
            )
            raise FlowDefinitionError(msg)
        target._dependency = expression

    def freeze(self, output: Any) -> _FrozenFlow:
        self.frozen = True
        outputs = self._validate_outputs(output)
        self._validate_nodes()
        topological_nodes = self._topological_sort()
        execution_nodes = self._execution_nodes(topological_nodes, outputs)
        return _FrozenFlow(
            nodes=tuple(self.nodes),
            topological_nodes=topological_nodes,
            execution_nodes=execution_nodes,
            outputs=outputs,
        )

    def _validate_outputs(self, output: Any) -> tuple[_FlowNodeInput[Any], ...]:
        if output is None:
            return ()
        outputs = output if isinstance(output, tuple) else (output,)
        for output_reference in outputs:
            if isinstance(output_reference, FlowNode):
                msg = (
                    "A durable DAG definition cannot return a FlowNode directly; "
                    "return node.outcome, node.error, node.result, or a tuple "
                    "of those projections."
                )
                raise FlowDefinitionError(msg)
            if not isinstance(output_reference, _FlowNodeInput):
                msg = (
                    "A durable DAG definition must return node.outcome, node.error, "
                    "node.result, a tuple of those projections, or None."
                )
                raise FlowDefinitionError(msg)
            if (
                output_reference.node._builder is not self
                or output_reference.node not in self.nodes
            ):
                msg = "Flow outputs must reference nodes from the current definition."
                raise InvalidStateError(msg)
        return outputs

    def _validate_nodes(self) -> None:
        names: set[str] = set()
        known_nodes = set(self.nodes)
        for flow_node in self.nodes:
            if not isinstance(flow_node.name, str) or not flow_node.name.strip():
                msg = "Flow node names must be non-empty strings."
                raise FlowDefinitionError(msg)
            if flow_node.name in names:
                msg = f"Flow node name {flow_node.name!r} is duplicated."
                raise FlowDefinitionError(msg)
            names.add(flow_node.name)
            if not callable(flow_node._func) or not getattr(
                flow_node._func, "_durable_node_callable", False
            ):
                msg = (
                    f"Flow node {flow_node.name!r} must use a bound callable "
                    "produced by @durable_node."
                )
                raise FlowDefinitionError(msg)

            dependency = flow_node._dependency
            if dependency is None:
                continue
            if dependency.builder is not self:
                msg = "Dependency expressions must belong to the current definition."
                raise InvalidStateError(msg)

            dependencies: set[FlowNode[Any]] = set()
            for leaf in dependency.leaves():
                if leaf.builder is not self or leaf.node not in known_nodes:
                    msg = f"Node {flow_node.name!r} references an unknown dependency."
                    raise FlowDefinitionError(msg)
                if leaf.node is flow_node:
                    msg = f"Flow node {flow_node.name!r} cannot depend on itself."
                    raise FlowDefinitionError(msg)
                if leaf.node in dependencies:
                    msg = (
                        f"Node {flow_node.name!r} contains duplicate dependency "
                        f"{leaf.node.name!r}."
                    )
                    raise FlowDefinitionError(msg)
                dependencies.add(leaf.node)

    def _topological_sort(self) -> tuple[FlowNode[Any], ...]:
        adjacency: dict[FlowNode[Any], list[FlowNode[Any]]] = {
            flow_node: [] for flow_node in self.nodes
        }
        indegree = {flow_node: 0 for flow_node in self.nodes}
        for target in self.nodes:
            if target._dependency is None:
                continue
            for leaf in target._dependency.leaves():
                adjacency[leaf.node].append(target)
                indegree[target] += 1

        for targets in adjacency.values():
            targets.sort(key=lambda flow_node: flow_node._index)

        ready = [
            flow_node._index for flow_node in self.nodes if indegree[flow_node] == 0
        ]
        heapq.heapify(ready)
        ordered: list[FlowNode[Any]] = []
        while ready:
            index = heapq.heappop(ready)
            flow_node = self.nodes[index]
            ordered.append(flow_node)
            for target in adjacency[flow_node]:
                indegree[target] -= 1
                if indegree[target] == 0:
                    heapq.heappush(ready, target._index)

        if len(ordered) != len(self.nodes):
            remaining = {
                flow_node for flow_node in self.nodes if indegree[flow_node] > 0
            }
            cycle = self._find_cycle(adjacency, remaining)
            cycle_path = " -> ".join(flow_node.name for flow_node in cycle)
            msg = f"Flow contains a cycle: {cycle_path}."
            raise FlowDefinitionError(msg)
        return tuple(ordered)

    def _execution_nodes(
        self,
        topological_nodes: tuple[FlowNode[Any], ...],
        outputs: tuple[_FlowNodeInput[Any], ...],
    ) -> tuple[FlowNode[Any], ...]:
        reachable = {output.node for output in outputs}
        pending = list(reachable)
        while pending:
            flow_node = pending.pop()
            expression = flow_node._dependency
            if expression is None:
                continue
            for leaf in expression.leaves():
                if leaf.node not in reachable:
                    reachable.add(leaf.node)
                    pending.append(leaf.node)
        return tuple(
            flow_node for flow_node in topological_nodes if flow_node in reachable
        )

    def _find_cycle(
        self,
        adjacency: Mapping[FlowNode[Any], list[FlowNode[Any]]],
        remaining: set[FlowNode[Any]],
    ) -> tuple[FlowNode[Any], ...]:
        state: dict[FlowNode[Any], int] = {}
        stack: list[FlowNode[Any]] = []
        stack_positions: dict[FlowNode[Any], int] = {}

        def visit(flow_node: FlowNode[Any]) -> tuple[FlowNode[Any], ...] | None:
            state[flow_node] = 1
            stack_positions[flow_node] = len(stack)
            stack.append(flow_node)
            for target in adjacency[flow_node]:
                if target not in remaining:
                    continue
                if state.get(target, 0) == 0:
                    cycle = visit(target)
                    if cycle is not None:
                        return cycle
                elif state[target] == 1:
                    start = stack_positions[target]
                    return tuple((*stack[start:], target))
            stack.pop()
            stack_positions.pop(flow_node)
            state[flow_node] = 2
            return None

        for flow_node in self.nodes:
            if flow_node in remaining and state.get(flow_node, 0) == 0:
                cycle = visit(flow_node)
                if cycle is not None:
                    return cycle

        msg = "Flow cycle detection failed to identify a concrete cycle."
        raise FlowDefinitionError(msg)


_current_flow_builder: ContextVar[_FlowBuilder | None] = ContextVar(
    "async_durable_execution.current_flow_builder",
    default=None,
)


def _require_active_builder(builder: _FlowBuilder) -> None:
    if _current_flow_builder.get() is not builder or builder.frozen:
        msg = "Flow nodes and dependency expressions may only be used in their definition."
        raise InvalidStateError(msg)


def _coerce_expression(
    value: FlowNode[Any] | _DependencyExpression,
) -> _DependencyExpression:
    if isinstance(value, FlowNode):
        return value.succeeded
    if isinstance(value, _DependencyExpression):
        return value
    msg = "Dependencies must be FlowNode handles or dependency expressions."
    raise FlowDefinitionError(msg)


def _flow_node_inputs(
    value: Any,
    *,
    active_containers: set[int] | None = None,
) -> tuple[_FlowNodeInput[Any], ...]:
    if isinstance(value, _FlowNodeInput):
        return (value,)
    if not isinstance(value, (list, tuple, dict)):
        hidden_references = _unsupported_flow_node_inputs(value)
        if hidden_references:
            container_type = type(value).__qualname__
            msg = (
                "Flow node projections nested in unsupported container type "
                f"{container_type!r} cannot be resolved. Use a list, tuple, or dict."
            )
            raise FlowDefinitionError(msg)
        return ()

    if active_containers is None:
        active_containers = set()
    container_id = id(value)
    if container_id in active_containers:
        msg = "Flow node arguments cannot contain recursive containers."
        raise FlowDefinitionError(msg)

    active_containers.add(container_id)
    try:
        values = (
            (*value.keys(), *value.values())
            if isinstance(value, dict)
            else tuple(value)
        )
        return tuple(
            reference
            for item in values
            for reference in _flow_node_inputs(
                item,
                active_containers=active_containers,
            )
        )
    finally:
        active_containers.remove(container_id)


def _unsupported_flow_node_inputs(
    value: Any,
    *,
    active_objects: set[int] | None = None,
) -> tuple[_FlowNodeInput[Any], ...]:
    if isinstance(value, _FlowNodeInput):
        return (value,)
    if isinstance(
        value,
        (
            str,
            bytes,
            bytearray,
            memoryview,
            range,
            FlowNode,
            _DependencyExpression,
        ),
    ) or callable(value):
        return ()
    if isinstance(value, Iterator):
        msg = (
            "Flow node arguments cannot use iterators because they cannot be "
            "inspected safely. Materialize the iterator as a list or tuple."
        )
        raise FlowDefinitionError(msg)

    if active_objects is None:
        active_objects = set()
    object_id = id(value)
    if object_id in active_objects:
        return ()

    active_objects.add(object_id)
    try:
        values: tuple[Any, ...] | None = None
        if isinstance(value, Mapping):
            values = (*value.keys(), *value.values())
        elif isinstance(value, (list, tuple)):
            values = tuple(value)
        elif isinstance(value, Set):
            values = tuple(value)
        elif is_dataclass(value) and not isinstance(value, type):
            values = tuple(
                getattr(value, dataclass_field.name)
                for dataclass_field in dataclass_fields(value)
            )
        else:
            attrs_fields = getattr(type(value), "__attrs_attrs__", None)
            if attrs_fields is not None:
                values = tuple(
                    getattr(value, attribute.name) for attribute in attrs_fields
                )
            elif isinstance(value, Iterable):
                container_type = type(value).__qualname__
                msg = (
                    "Flow node arguments cannot use unsupported container type "
                    f"{container_type!r}. Use a list, tuple, or dict."
                )
                raise FlowDefinitionError(msg)
            elif hasattr(value, "__dict__"):
                values = tuple(vars(value).values())
            else:
                slots = getattr(type(value), "__slots__", ())
                if isinstance(slots, str):
                    slots = (slots,)
                slot_values = tuple(
                    getattr(value, slot)
                    for slot in slots
                    if slot not in {"__dict__", "__weakref__"} and hasattr(value, slot)
                )
                if slot_values:
                    values = slot_values

        if values is None:
            return ()
        return tuple(
            reference
            for item in values
            for reference in _unsupported_flow_node_inputs(
                item,
                active_objects=active_objects,
            )
        )
    finally:
        active_objects.remove(object_id)


def _bound_flow_node_inputs(
    func: Callable[[], Awaitable[Any]],
) -> tuple[_FlowNodeInput[Any], ...]:
    args = cast("tuple[Any, ...]", getattr(func, "_durable_node_args"))
    kwargs = cast("Mapping[str, Any]", getattr(func, "_durable_node_kwargs"))
    return tuple(
        reference
        for value in (*args, *kwargs.values())
        for reference in _flow_node_inputs(value)
    )


def _input_dependency_expression(
    builder: _FlowBuilder,
    func: Callable[[], Awaitable[Any]],
) -> _DependencyExpression | None:
    references: dict[FlowNode[Any], _FlowNodeInput[Any]] = {}
    for reference in _bound_flow_node_inputs(func):
        if reference.node._builder is not builder:
            msg = "Flow node inputs must come from the current flow definition."
            raise InvalidStateError(msg)
        existing = references.get(reference.node)
        if existing is not None and existing.kind is not reference.kind:
            msg = (
                f"Flow node input {reference.node.name!r} cannot require multiple "
                "projections."
            )
            raise FlowDefinitionError(msg)
        references[reference.node] = reference

    leaves = tuple(
        _DependencyLeaf(builder, reference.node, reference.condition)
        for reference in sorted(
            references.values(),
            key=lambda item: item.node._index,
        )
    )
    if not leaves:
        return None
    if len(leaves) == 1:
        return leaves[0]
    return _CompositeDependencyExpression(
        builder=builder,
        mode=_DependencyMode.ALL,
        children=leaves,
    )


def node(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    dependency: FlowNode[Any] | _DependencyExpression | None = None,
) -> FlowNode[T]:
    """Declare a node and derive required dependencies from its bound inputs."""
    builder = _current_flow_builder.get()
    if builder is None or builder.frozen:
        msg = "node() can only be used while a @durable_dag definition is evaluating."
        raise InvalidStateError(msg)
    required_metadata = (
        "_durable_node_function",
        "_durable_node_args",
        "_durable_node_kwargs",
    )
    if (
        not callable(func)
        or not getattr(func, "_durable_node_callable", False)
        or any(not hasattr(func, attribute) for attribute in required_metadata)
    ):
        msg = "node() requires a bound callable produced by @durable_node."
        raise FlowDefinitionError(msg)

    node_name = name if name is not None else getattr(func, "__name__", None)
    input_expression = _input_dependency_expression(builder, func)
    explicit_expression = (
        _coerce_expression(dependency) if dependency is not None else None
    )

    expression: _DependencyExpression | None
    if input_expression is not None and explicit_expression is not None:
        input_nodes = {leaf.node for leaf in input_expression.leaves()}
        duplicated = next(
            (
                leaf.node
                for leaf in explicit_expression.leaves()
                if leaf.node in input_nodes
            ),
            None,
        )
        if duplicated is not None:
            msg = (
                f"Node {node_name!r} declares {duplicated.name!r} as both "
                "a required input and an explicit dependency."
            )
            raise FlowDefinitionError(msg)
        expression = input_expression & explicit_expression
    else:
        expression = input_expression or explicit_expression

    flow_node = builder.add_node(func, cast("str", node_name))
    if expression is not None:
        builder.add_dependency(flow_node, expression)
    return flow_node


def durable_node(
    func: Callable[Params, Awaitable[T]],
) -> Callable[Params, Callable[[], Awaitable[T]]]:
    """Bind arguments to an async function used as a durable flow node."""
    if isinstance(func, classmethod):
        return classmethod(durable_node(func.__func__))
    if isinstance(func, staticmethod):
        return staticmethod(durable_node(func.__func__))
    if not inspect.iscoroutinefunction(func):
        msg = "@durable_node can only decorate an async node function."
        raise FlowDefinitionError(msg)

    @functools.wraps(func)
    def wrapper(
        *args: Params.args, **kwargs: Params.kwargs
    ) -> Callable[[], Awaitable[T]]:
        inspect.signature(func).bind(*args, **kwargs)
        bound = functools.partial(func, *args, **kwargs)
        setattr(bound, "__name__", func.__name__)
        setattr(bound, "_durable_node_callable", True)
        setattr(bound, "_durable_node_function", func)
        setattr(bound, "_durable_node_args", args)
        setattr(bound, "_durable_node_kwargs", kwargs)
        return bound

    setattr(wrapper, "_durable_node", True)
    return wrapper


def durable_dag(
    func: Callable[Params, Any],
) -> Callable[Params, Callable[[], Any]]:
    """Bind arguments to a synchronous declarative flow definition."""
    if isinstance(func, classmethod):
        return classmethod(durable_dag(func.__func__))
    if isinstance(func, staticmethod):
        return staticmethod(durable_dag(func.__func__))
    if inspect.iscoroutinefunction(func):
        msg = "@durable_dag can only decorate a synchronous definition function."
        raise FlowDefinitionError(msg)

    @functools.wraps(func)
    def wrapper(*args: Params.args, **kwargs: Params.kwargs) -> Callable[[], Any]:
        bound = functools.partial(func, *args, **kwargs)
        setattr(bound, "__name__", func.__name__)
        setattr(bound, "_durable_dag_definition", True)
        return bound

    setattr(wrapper, "_durable_dag", True)
    return wrapper


@dataclass(frozen=True)
class _NodeExecution:
    result: FlowNodeResult[Any]
    handled_failures: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "result": _flow_node_result_to_checkpoint_dict(self.result),
            "handledFailures": list(self.handled_failures),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> _NodeExecution:
        return cls(
            result=FlowNodeResult.from_dict(data["result"]),
            handled_failures=tuple(
                str(name) for name in data.get("handledFailures", ())
            ),
        )


class _FlowValueSerDes(SerDes[Any]):
    def __init__(self) -> None:
        self.delegate: SerDes[Any] = _BatchResultSerDes()

    async def serialize(self, value: Any) -> str:
        return await self.delegate.serialize(_encode_flow_value(value))

    async def deserialize(self, data: str) -> Any:
        return _decode_flow_value(await self.delegate.deserialize(data))


class _NodeExecutionSerDes(SerDes[_NodeExecution]):
    def __init__(self) -> None:
        self.delegate = _FlowValueSerDes()

    async def serialize(self, value: _NodeExecution) -> str:
        return await self.delegate.serialize(value.to_dict())

    async def deserialize(self, data: str) -> _NodeExecution:
        decoded = await self.delegate.deserialize(data)
        if not isinstance(decoded, Mapping):
            msg = "Serialized flow node result must be a mapping."
            raise SerDesError(msg)
        return _NodeExecution.from_dict(decoded)


@dataclass(frozen=True)
class _PersistedDependencyResolution:
    matched: bool
    selected_nodes: tuple[str, ...]
    handled_failures: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "matched": self.matched,
            "selectedNodes": list(self.selected_nodes),
            "handledFailures": list(self.handled_failures),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> _PersistedDependencyResolution:
        return cls(
            matched=bool(data["matched"]),
            selected_nodes=tuple(str(name) for name in data["selectedNodes"]),
            handled_failures=tuple(
                str(name) for name in data.get("handledFailures", ())
            ),
        )


class _PersistedDependencyResolutionSerDes(SerDes[_PersistedDependencyResolution]):
    def __init__(self) -> None:
        self.delegate: ExtendedTypeSerDes[Any] = ExtendedTypeSerDes()

    async def serialize(self, value: _PersistedDependencyResolution) -> str:
        return await self.delegate.serialize(value.to_dict())

    async def deserialize(self, data: str) -> _PersistedDependencyResolution:
        decoded = await self.delegate.deserialize(data)
        if not isinstance(decoded, Mapping):
            msg = "Serialized flow dependency resolution must be a mapping."
            raise SerDesError(msg)
        return _PersistedDependencyResolution.from_dict(decoded)


class _FlowResultSerDes(SerDes[FlowResult]):
    def __init__(self) -> None:
        self.delegate = _FlowValueSerDes()

    async def serialize(self, value: FlowResult) -> str:
        return await self.delegate.serialize(_flow_result_to_checkpoint_dict(value))

    async def deserialize(self, data: str) -> FlowResult:
        decoded = await self.delegate.deserialize(data)
        if not isinstance(decoded, Mapping):
            msg = "Serialized flow result must be a mapping."
            raise SerDesError(msg)
        return FlowResult.from_dict(decoded)


_NODE_EXECUTION_SERDES = _NodeExecutionSerDes()
_DEPENDENCY_RESOLUTION_SERDES = _PersistedDependencyResolutionSerDes()
_FLOW_RESULT_SERDES = _FlowResultSerDes()
_FLOW_VALUE_SERDES = _FlowValueSerDes()


async def _clone_flow_value(value: Any) -> Any:
    """Clone a value using the same representation as flow checkpoints."""
    return await _FLOW_VALUE_SERDES.deserialize(
        await _FLOW_VALUE_SERDES.serialize(value)
    )


async def _clone_dependency_results(
    results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
) -> dict[FlowNode[Any], FlowNodeResult[Any]]:
    """Clone settled results once for one consumer using checkpoint semantics."""
    cloned: dict[FlowNode[Any], FlowNodeResult[Any]] = {}
    for flow_node, result in results.items():
        value = await _clone_flow_value(result)
        if not isinstance(value, FlowNodeResult):
            msg = "Cloned flow dependency result has an invalid type."
            raise SerDesError(msg)
        cloned[flow_node] = value
    return cloned


class _FlowControlSignal(BaseException):
    """Carry SDK control failures through child executors without checkpointing."""

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


@dataclass(frozen=True)
class _DependencyResolution:
    matched: bool
    results: Mapping[FlowNode[Any], FlowNodeResult[Any]]
    handled_failures: tuple[FlowNode[Any], ...] = ()


def _callable_error_object(error: Exception) -> ErrorObject:
    if isinstance(error, CallableRuntimeError):
        return ErrorObject(
            message=error.message,
            type=error.error_type,
            data=error.data,
            stack_trace=error.stack_trace,
        )
    return ErrorObject.from_exception(error)


def _find_control_error(error: Exception) -> Exception | None:
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if not isinstance(current, Exception) or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, (ExecutionError, InvocationError, SerDesError)):
            return current
        if isinstance(current, CallableRuntimeError):
            error_type = current.error_type or ""
            message = current.message or str(current)
            control_error = _restore_sdk_control_error(
                message,
                current.error_type,
                current.data,
            )
            if control_error is not None:
                return control_error
        related: list[BaseException] = []
        if _BASE_EXCEPTION_GROUP_TYPE is not None and isinstance(
            current,
            _BASE_EXCEPTION_GROUP_TYPE,
        ):
            related.extend(
                nested
                for nested in getattr(current, "exceptions", ())
                if isinstance(nested, BaseException)
            )
        cause = current.__cause__ or current.__context__
        if cause is not None:
            related.append(cause)
        pending.extend(reversed(related))
    return None


async def _await_node_execution(
    task: asyncio.Task[_NodeExecution],
) -> _NodeExecution:
    try:
        return await asyncio.shield(task)
    except _FlowControlSignal:
        raise
    except Exception as error:
        control_error = _find_control_error(error)
        if control_error is not None:
            raise _FlowControlSignal(control_error) from error
        raise


async def _await_resolver_resolution(
    task: asyncio.Task[_PersistedDependencyResolution],
    expression: _DependencyExpression,
    tasks: Mapping[FlowNode[Any], asyncio.Task[_NodeExecution]],
) -> _DependencyResolution:
    try:
        persisted = await asyncio.shield(task)
    except _FlowControlSignal:
        raise
    except Exception as error:
        control_error = _find_control_error(error)
        if control_error is not None:
            raise _FlowControlSignal(control_error) from error
        raise

    nodes_by_name = {leaf.node.name: leaf.node for leaf in expression.leaves()}
    selected: dict[FlowNode[Any], FlowNodeResult[Any]] = {}
    for node_name in persisted.selected_nodes:
        flow_node = nodes_by_name[node_name]
        execution = await _await_node_execution(tasks[flow_node])
        selected[flow_node] = execution.result

    return _DependencyResolution(
        matched=persisted.matched,
        results=selected,
        handled_failures=tuple(
            nodes_by_name[node_name] for node_name in persisted.handled_failures
        ),
    )


def _persisted_resolution(
    resolution: _DependencyResolution,
) -> _PersistedDependencyResolution:
    return _PersistedDependencyResolution(
        matched=resolution.matched,
        selected_nodes=tuple(node.name for node in resolution.results),
        handled_failures=tuple(node.name for node in resolution.handled_failures),
    )


def _raise_task_error(value: BaseException) -> NoReturn:
    if isinstance(value, _FlowControlSignal):
        raise value
    if isinstance(value, Exception):
        control_error = _find_control_error(value)
        if control_error is not None:
            raise _FlowControlSignal(control_error) from value
        raise value
    raise value


def _raise_collected_task_errors(values: Iterable[object]) -> None:
    suspensions: list[SuspendExecution] = []
    for value in values:
        if isinstance(value, SuspendExecution):
            suspensions.append(value)
        elif isinstance(value, _FlowControlSignal):
            raise value
        elif isinstance(value, Exception):
            control_error = _find_control_error(value)
            if control_error is not None:
                raise _FlowControlSignal(control_error) from value
            raise _FlowControlSignal(value) from value
        elif isinstance(value, BaseException):
            raise value

    timed_suspensions = [
        suspension
        for suspension in suspensions
        if isinstance(suspension, TimedSuspendExecution)
    ]
    if timed_suspensions:
        raise min(
            timed_suspensions,
            key=lambda suspension: suspension.scheduled_timestamp,
        )
    if suspensions:
        raise suspensions[0]


async def _resolve_dependency_expression(
    target: FlowNode[Any],
    expression: _DependencyExpression,
    tasks: Mapping[FlowNode[Any], asyncio.Task[_NodeExecution]],
    resolver_tasks: Mapping[
        tuple[FlowNode[Any], int],
        asyncio.Task[_PersistedDependencyResolution],
    ],
) -> _DependencyResolution:
    if isinstance(expression, _DependencyLeaf):
        execution = await _await_node_execution(tasks[expression.node])
        result = execution.result
        evaluation = expression.evaluate({expression.node: result})
        return _DependencyResolution(
            matched=evaluation.status is _EvaluationStatus.MATCHED,
            results={expression.node: result},
            handled_failures=evaluation.handled_failures,
        )

    composite = cast(_CompositeDependencyExpression, expression)
    if composite.mode is _DependencyMode.ANY:
        return await _await_resolver_resolution(
            resolver_tasks[(target, id(composite))],
            composite,
            tasks,
        )

    child_tasks: list[asyncio.Task[_DependencyResolution]] = []
    for child_expression in composite.children:

        async def resolve_child(
            current_child: _DependencyExpression = child_expression,
        ) -> _DependencyResolution:
            return await _resolve_dependency_expression(
                target,
                current_child,
                tasks,
                resolver_tasks,
            )

        child_tasks.append(create_eager_task(resolve_child))

    values = await asyncio.gather(*child_tasks, return_exceptions=True)
    children: list[_DependencyResolution] = []
    for value in values:
        if isinstance(value, BaseException):
            _raise_task_error(value)
        children.append(value)

    selected: dict[FlowNode[Any], FlowNodeResult[Any]] = {}
    for child_resolution in children:
        selected.update(child_resolution.results)
    if any(not child_resolution.matched for child_resolution in children):
        return _DependencyResolution(matched=False, results=selected)
    return _DependencyResolution(
        matched=True,
        results=selected,
        handled_failures=tuple(
            dependency
            for child_resolution in children
            for dependency in child_resolution.handled_failures
        ),
    )


async def _resolve_any_expression(
    target: FlowNode[Any],
    expression: _CompositeDependencyExpression,
    tasks: Mapping[FlowNode[Any], asyncio.Task[_NodeExecution]],
    resolver_tasks: Mapping[
        tuple[FlowNode[Any], int],
        asyncio.Task[_PersistedDependencyResolution],
    ],
) -> _PersistedDependencyResolution:
    children: list[asyncio.Task[_DependencyResolution]] = []
    completed: asyncio.Queue[int] = asyncio.Queue()
    for index, child_expression in enumerate(expression.children):

        async def resolve_child(
            current_child: _DependencyExpression = child_expression,
            current_index: int = index,
        ) -> _DependencyResolution:
            try:
                return await _resolve_dependency_expression(
                    target,
                    current_child,
                    tasks,
                    resolver_tasks,
                )
            finally:
                completed.put_nowait(current_index)

        children.append(create_eager_task(resolve_child))

    pending = set(range(len(children)))
    unmatched: dict[FlowNode[Any], FlowNodeResult[Any]] = {}
    suspensions: list[SuspendExecution] = []
    try:
        while pending:
            index = await completed.get()
            pending.remove(index)
            child_task = children[index]
            try:
                resolution = child_task.result()
            except SuspendExecution as error:
                suspensions.append(error)
                continue
            except BaseException as error:
                _raise_task_error(error)
            if resolution.matched:
                return _persisted_resolution(
                    _DependencyResolution(
                        matched=True,
                        results={**unmatched, **resolution.results},
                        handled_failures=resolution.handled_failures,
                    )
                )
            unmatched.update(resolution.results)
        if suspensions:
            timed_suspensions = [
                error
                for error in suspensions
                if isinstance(error, TimedSuspendExecution)
            ]
            if timed_suspensions:
                raise min(
                    timed_suspensions,
                    key=lambda error: error.scheduled_timestamp,
                )
            raise suspensions[0]
        return _persisted_resolution(
            _DependencyResolution(matched=False, results=unmatched)
        )
    finally:
        for index in pending:
            children[index].cancel()
        if pending:
            await asyncio.gather(
                *(children[index] for index in pending),
                return_exceptions=True,
            )


def _any_expressions(
    expression: _DependencyExpression,
) -> tuple[_CompositeDependencyExpression, ...]:
    if isinstance(expression, _DependencyLeaf):
        return ()
    composite = cast(_CompositeDependencyExpression, expression)
    nested = tuple(
        nested_expression
        for child in composite.children
        for nested_expression in _any_expressions(child)
    )
    if composite.mode is _DependencyMode.ANY:
        return (*nested, composite)
    return nested


def _flow_node_context(
    context: DurableContext,
    direct_dependencies: frozenset[FlowNode[Any]],
    dependency_results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
) -> FlowNodeContext:
    flow_context = FlowNodeContext(
        execution_state=context.execution_state,
        operation_identifier=context.operation_identifier,
        step_id_prefix=context.step_id_prefix,
        replaying=context.is_replaying(),
        _direct_dependencies=direct_dependencies,
        _dependency_results=dependency_results,
    )
    if "step_counter" in context.__dict__:
        flow_context.__dict__["step_counter"] = context.__dict__["step_counter"]
    return flow_context


def _resolve_flow_node_inputs(
    value: Any,
    results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
) -> Any:
    if isinstance(value, _FlowNodeInput):
        return value.resolve(results)
    if isinstance(value, list):
        resolved_list = [_resolve_flow_node_inputs(item, results) for item in value]
        return (
            value
            if all(a is b for a, b in zip(resolved_list, value, strict=True))
            else resolved_list
        )
    if isinstance(value, tuple):
        resolved_tuple = tuple(
            _resolve_flow_node_inputs(item, results) for item in value
        )
        if all(a is b for a, b in zip(resolved_tuple, value, strict=True)):
            return value
        if hasattr(value, "_fields"):
            return type(value)(*resolved_tuple)
        return resolved_tuple
    if isinstance(value, dict):
        resolved_items = tuple(
            (
                _resolve_flow_node_inputs(key, results),
                _resolve_flow_node_inputs(item, results),
            )
            for key, item in value.items()
        )
        if all(
            resolved_key is key and resolved_value is item
            for (resolved_key, resolved_value), (key, item) in zip(
                resolved_items,
                value.items(),
                strict=True,
            )
        ):
            return value
        return dict(resolved_items)
    return value


async def _clone_flow_node_arguments(
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
    results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Clone one node's complete argument graph at its execution boundary."""
    resolved_args = tuple(_resolve_flow_node_inputs(value, results) for value in args)
    resolved_kwargs = {
        key: _resolve_flow_node_inputs(value, results) for key, value in kwargs.items()
    }
    cloned = await _clone_flow_value((resolved_args, resolved_kwargs))
    if not isinstance(cloned, tuple) or len(cloned) != 2:
        msg = "Cloned flow node arguments have an invalid structure."
        raise SerDesError(msg)
    cloned_args, cloned_kwargs = cloned
    if not isinstance(cloned_args, tuple) or not isinstance(cloned_kwargs, dict):
        msg = "Cloned flow node arguments have an invalid structure."
        raise SerDesError(msg)
    restored_args = cast(
        "tuple[Any, ...]",
        _restore_flow_node_input_references(args, cloned_args, results),
    )
    restored_kwargs = cast(
        "dict[str, Any]",
        _restore_flow_node_input_references(dict(kwargs), cloned_kwargs, results),
    )
    return restored_args, restored_kwargs


def _restore_flow_node_input_references(
    template: Any,
    cloned: Any,
    results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
) -> Any:
    """Rebind projections after cloning while retaining cloned container values."""
    if isinstance(template, _FlowNodeInput):
        return template.resolve(results)
    if isinstance(template, list) and isinstance(cloned, list):
        return [
            _restore_flow_node_input_references(source, value, results)
            for source, value in zip(template, cloned, strict=True)
        ]
    if isinstance(template, tuple) and isinstance(cloned, tuple):
        restored = tuple(
            _restore_flow_node_input_references(source, value, results)
            for source, value in zip(template, cloned, strict=True)
        )
        if hasattr(template, "_fields"):
            return type(template)(*restored)
        return restored
    if isinstance(template, dict) and isinstance(cloned, dict):
        return dict(
            (
                _restore_flow_node_input_references(
                    source_key,
                    cloned_key,
                    results,
                ),
                _restore_flow_node_input_references(
                    source_value,
                    cloned_value,
                    results,
                ),
            )
            for (source_key, source_value), (cloned_key, cloned_value) in zip(
                template.items(),
                cloned.items(),
                strict=True,
            )
        )
    return cloned


async def _invoke_flow_node(
    flow_node: FlowNode[Any],
    results: Mapping[FlowNode[Any], FlowNodeResult[Any]],
) -> Any:
    func = cast(
        "Callable[..., Awaitable[Any]]",
        getattr(flow_node._func, "_durable_node_function"),
    )
    args = cast("tuple[Any, ...]", getattr(flow_node._func, "_durable_node_args"))
    kwargs = cast(
        "Mapping[str, Any]",
        getattr(flow_node._func, "_durable_node_kwargs"),
    )
    cloned_args, cloned_kwargs = await _clone_flow_node_arguments(
        args,
        kwargs,
        results,
    )
    return await func(*cloned_args, **cloned_kwargs)


async def _execute_node(
    flow_node: FlowNode[Any],
    tasks: Mapping[FlowNode[Any], asyncio.Task[_NodeExecution]],
    resolver_tasks: Mapping[
        tuple[FlowNode[Any], int],
        asyncio.Task[_PersistedDependencyResolution],
    ],
    resolver_ready: asyncio.Event,
) -> _NodeExecution:
    expression = flow_node._dependency
    if expression is None:
        resolution = _DependencyResolution(matched=True, results={})
        direct_dependencies: frozenset[FlowNode[Any]] = frozenset()
    else:
        await resolver_ready.wait()
        resolution = await _resolve_dependency_expression(
            flow_node,
            expression,
            tasks,
            resolver_tasks,
        )
        direct_dependencies = frozenset(leaf.node for leaf in expression.leaves())

    handled_failures = tuple(
        dependency.name for dependency in resolution.handled_failures
    )
    if not resolution.matched:
        return _NodeExecution(
            result=FlowNodeResult.skipped(),
            handled_failures=handled_failures,
        )

    consumer_results = await _clone_dependency_results(resolution.results)
    context = get_durable_context()
    flow_context = _flow_node_context(
        context,
        direct_dependencies,
        consumer_results,
    )
    try:
        with bind_current_context(flow_context):
            outcome = await _invoke_flow_node(flow_node, consumer_results)
        return _NodeExecution(
            result=FlowNodeResult.succeeded(outcome),
            handled_failures=handled_failures,
        )
    except Exception as error:
        control_error = _find_control_error(error)
        if control_error is not None:
            raise _FlowControlSignal(control_error) from error
        return _NodeExecution(
            result=FlowNodeResult.failed(_callable_error_object(error)),
            handled_failures=handled_failures,
        )


async def _execute_flow(frozen_flow: _FrozenFlow) -> FlowResult:
    tasks: dict[FlowNode[Any], asyncio.Task[_NodeExecution]] = {}
    resolver_tasks: dict[
        tuple[FlowNode[Any], int],
        asyncio.Task[_PersistedDependencyResolution],
    ] = {}
    resolver_ready = asyncio.Event()
    for flow_node in frozen_flow.execution_nodes:

        async def run_node(current_node: FlowNode[Any] = flow_node) -> _NodeExecution:
            return await _execute_node(
                current_node,
                tasks,
                resolver_tasks,
                resolver_ready,
            )

        tasks[flow_node] = run_in_child_context(
            run_node,
            name=flow_node.name,
            serdes=_NODE_EXECUTION_SERDES,
        )

    resolver_task_order: list[asyncio.Task[_PersistedDependencyResolution]] = []
    for flow_node in frozen_flow.execution_nodes:
        expression = flow_node._dependency
        if expression is None:
            continue
        for ordinal, any_expression in enumerate(
            _any_expressions(expression),
            start=1,
        ):

            async def run_resolver(
                target: FlowNode[Any] = flow_node,
                current_expression: _CompositeDependencyExpression = any_expression,
            ) -> _PersistedDependencyResolution:
                await resolver_ready.wait()
                return await _resolve_any_expression(
                    target,
                    current_expression,
                    tasks,
                    resolver_tasks,
                )

            resolver_task = run_in_child_context(
                run_resolver,
                name=f"flow-any-resolution-{flow_node.name}-{ordinal}",
                serdes=_DEPENDENCY_RESOLUTION_SERDES,
            )
            resolver_tasks[(flow_node, id(any_expression))] = resolver_task
            resolver_task_order.append(resolver_task)

    resolver_ready.set()
    values = await asyncio.gather(
        *tasks.values(),
        *resolver_task_order,
        return_exceptions=True,
    )
    _raise_collected_task_errors(values)
    node_values = values[: len(tasks)]
    execution_nodes = set(frozen_flow.execution_nodes)
    executions = {
        flow_node: _NodeExecution(result=FlowNodeResult.skipped())
        for flow_node in frozen_flow.nodes
        if flow_node not in execution_nodes
    }
    for flow_node, value in zip(
        frozen_flow.execution_nodes,
        node_values,
        strict=True,
    ):
        executions[flow_node] = cast("_NodeExecution", value)

    results = {
        flow_node.name: executions[flow_node].result for flow_node in frozen_flow.nodes
    }
    handled_failures = {
        name for execution in executions.values() for name in execution.handled_failures
    }
    handled_failures.update(
        output.node.name
        for output in frozen_flow.outputs
        if output.kind in {_FlowNodeInputKind.ERROR, _FlowNodeInputKind.RESULT}
        and executions[output.node].result.status is FlowNodeStatus.FAILED
    )
    unhandled_failures = tuple(
        flow_node.name
        for flow_node in frozen_flow.nodes
        if executions[flow_node].result.status is FlowNodeStatus.FAILED
        and flow_node.name not in handled_failures
    )
    unavailable_outputs = tuple(
        dict.fromkeys(
            output.node.name
            for output in frozen_flow.outputs
            if output.kind is _FlowNodeInputKind.OUTCOME
            and executions[output.node].result.status is not FlowNodeStatus.SUCCEEDED
        )
    )
    return FlowResult(
        results=results,
        outputs=tuple(
            output.project(executions[output.node].result)
            for output in frozen_flow.outputs
        ),
        unhandled_failures=unhandled_failures,
        unavailable_outputs=unavailable_outputs,
        _output_kinds=tuple(output.kind for output in frozen_flow.outputs),
    )


def _evaluate_definition(definition: Callable[[], Any]) -> _FrozenFlow:
    if not getattr(definition, "_durable_dag_definition", False):
        msg = "flow() requires a bound callable produced by @durable_dag."
        raise FlowDefinitionError(msg)

    builder = _FlowBuilder()
    token = _current_flow_builder.set(builder)
    try:
        with bind_durable_definition("flow"):
            output = definition()
    finally:
        _current_flow_builder.reset(token)

    if inspect.isawaitable(output):
        if inspect.iscoroutine(output):
            output.close()
        msg = "A durable DAG definition must execute synchronously."
        raise FlowDefinitionError(msg)
    return builder.freeze(output)


def flow(
    definition: Callable[[], Any],
    *,
    name: str | None = None,
) -> asyncio.Task[FlowResult]:
    """Validate and start a declarative acyclic durable workflow."""
    ensure_durable_operations_allowed("flow")
    get_durable_context()
    frozen_flow = _evaluate_definition(definition)
    flow_name = name or getattr(definition, "__name__", None) or "flow"
    child_task = run_in_child_context(
        functools.partial(_execute_flow, frozen_flow),
        name=flow_name,
        serdes=_FLOW_RESULT_SERDES,
    )

    async def finish_flow() -> FlowResult:
        try:
            result = await child_task
        except _FlowControlSignal as signal:
            raise signal.error
        except Exception as error:
            control_error = _find_control_error(error)
            if control_error is not None:
                raise control_error from error
            raise

        problems: list[str] = []
        if result.has_unhandled_failures:
            failures = ", ".join(result.unhandled_failures)
            problems.append(f"unhandled node failures: {failures}")
        if result.has_unavailable_outputs:
            outputs = ", ".join(result.unavailable_outputs)
            problems.append(
                f"unavailable outcome outputs: {outputs}; return node.result "
                "for conditional outputs"
            )
        if problems:
            msg = f"Flow has {'; '.join(problems)}."
            raise FlowExecutionError(msg, result)
        return result

    return create_eager_task(finish_flow)

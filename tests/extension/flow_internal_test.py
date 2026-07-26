"""Focused tests for flow definition validation and result helpers."""

from __future__ import annotations

import asyncio
import builtins
import json
from collections import deque
from dataclasses import dataclass
from typing import Any, NamedTuple, cast
from unittest.mock import Mock

import pytest

from async_durable_execution import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
    DurableContext,
    ErrorObject,
    FlowDefinitionError,
    FlowNodeResult,
    FlowNodeStatus,
    FlowResult,
    InvalidStateError,
    durable_dag,
    durable_node,
    flow,
    get_node_context,
    node,
)
from async_durable_execution.extension.flow import (
    FlowNode,
    _DependencyResolution,
    _FlowBuilder,
    _FlowControlSignal,
    _FlowResultSerDes,
    _FlowValueSerDes,
    _FlowNodeInput,
    _FlowNodeInputKind,
    _NodeExecution,
    _NodeExecutionSerDes,
    _PersistedDependencyResolutionSerDes,
    _await_resolver_resolution,
    _coerce_expression,
    _clone_dependency_results,
    _clone_flow_node_arguments,
    _decode_flow_value,
    _encode_flow_value,
    _evaluate_definition,
    _execute_flow,
    _find_control_error,
    _flow_node_inputs,
    _flow_node_context,
    _raise_collected_task_errors,
    _raise_task_error,
    _resolve_any_expression,
    _resolve_flow_node_inputs,
    _resolve_dependency_expression,
)
from async_durable_execution.context import bind_current_context
from async_durable_execution.exceptions import (
    BotoClientError,
    CallableRuntimeError,
    DurableApiErrorCategory,
    ExecutionError,
    InvocationError,
    SerDesError,
    SuspendExecution,
    TerminationReason,
    TimedSuspendExecution,
    _decode_sdk_error_data,
    _encode_sdk_control_error_data,
    _encode_sdk_error_data,
    _sdk_error_type_name,
)
from async_durable_execution.primitive.callback import CallbackError
from async_durable_execution.execution import handle_user_function_exception
from async_durable_execution.models import (
    InvocationStatus,
    OperationIdentifier,
    OperationSubType,
)
from async_durable_execution.serdes import ExtendedTypeSerDes
from async_durable_execution.state import ExecutionState


@durable_node
async def return_name() -> str:
    return get_node_context().operation_name or ""


class _FatalFlowSignal(BaseException):
    pass


class _CustomInvocationError(InvocationError):
    def __init__(self, message: str, *, retryable: bool):
        super().__init__(message, TerminationReason.STEP_INTERRUPTED)
        self._retryable = retryable

    def is_retryable(self) -> bool:
        return self._retryable


class _CustomExecutionError(ExecutionError):
    pass


class _CustomSerDesError(SerDesError):
    pass


def test_flow_result_helpers_preserve_selected_output_arity():
    first = FlowNodeResult.succeeded("first")
    second = FlowNodeResult.succeeded("second")

    empty = FlowResult(results={})
    single = FlowResult(results={"first": first}, outputs=(first,))
    multiple = FlowResult(
        results={"first": first, "second": second},
        outputs=(first, second),
    )

    assert empty.output is None
    assert single.output is first
    assert multiple.output == (first, second)
    assert multiple.get_result("second") is second
    with pytest.raises(KeyError, match="missing"):
        multiple.get_result("missing")
    assert FlowResult.from_dict(multiple.to_dict()) == multiple


async def test_dependency_results_are_cloned_per_consumer():
    builder = _FlowBuilder()
    source = builder.add_node(return_name(), "source")
    original = FlowNodeResult.succeeded(
        {
            "items": ["original"],
            "nested": FlowNodeResult.succeeded("value"),
        }
    )

    first = await _clone_dependency_results({source: original})
    second = await _clone_dependency_results({source: original})

    first_result = first[source]
    second_result = second[source]
    assert first_result == second_result == original
    assert first_result is not original
    assert second_result is not original
    assert first_result.outcome is not original.outcome
    assert second_result.outcome is not original.outcome
    assert isinstance(first_result.outcome, dict)
    assert isinstance(second_result.outcome, dict)
    assert isinstance(first_result.outcome["nested"], FlowNodeResult)

    first_result.outcome["items"].append("first")
    assert original.outcome == {
        "items": ["original"],
        "nested": FlowNodeResult.succeeded("value"),
    }
    assert second_result.outcome == original.outcome


async def test_dependency_result_clone_rejects_invalid_decoded_value(monkeypatch):
    async def deserialize_invalid_value(self, data):
        return "invalid"

    monkeypatch.setattr(
        _FlowValueSerDes,
        "deserialize",
        deserialize_invalid_value,
    )
    builder = _FlowBuilder()
    source = builder.add_node(return_name(), "source")

    with pytest.raises(
        SerDesError,
        match="Cloned flow dependency result has an invalid type",
    ):
        await _clone_dependency_results({source: FlowNodeResult.succeeded("original")})


async def test_flow_node_arguments_are_cloned_as_one_complete_graph():
    class Metadata(NamedTuple):
        label: str
        values: dict[str, str]

    metadata = Metadata("test", {"state": "original"})
    args = ({"items": ["original"]}, metadata)
    kwargs = {"metadata": {"state": "original"}}

    cloned_args, cloned_kwargs = await _clone_flow_node_arguments(args, kwargs, {})

    assert cloned_args == args
    assert cloned_kwargs == kwargs
    assert cloned_args is not args
    assert cloned_kwargs is not kwargs
    assert cloned_args[0] is not args[0]
    assert cloned_kwargs["metadata"] is not kwargs["metadata"]
    cloned_metadata = cast("Metadata", cloned_args[1])
    assert isinstance(cloned_metadata, Metadata)
    assert cloned_metadata.values is not metadata.values


@pytest.mark.parametrize("invalid_value", ["invalid", ((), [])])
async def test_flow_node_argument_clone_rejects_invalid_structure(
    monkeypatch,
    invalid_value,
):
    async def deserialize_invalid_value(self, data):
        return invalid_value

    monkeypatch.setattr(
        _FlowValueSerDes,
        "deserialize",
        deserialize_invalid_value,
    )

    with pytest.raises(
        SerDesError,
        match="Cloned flow node arguments have an invalid structure",
    ):
        await _clone_flow_node_arguments((), {}, {})


def test_flow_result_projection_serialization_and_validation():
    first = FlowNodeResult.succeeded("first")
    legacy = {
        "results": {"first": first.to_dict()},
        "outputs": [first.to_dict()],
        "unhandledFailures": [],
    }
    restored_legacy = FlowResult.from_dict(legacy)
    assert restored_legacy.outputs == (first,)

    error_output = FlowResult(
        results={},
        outputs=(None,),
        _output_kinds=(_FlowNodeInputKind.ERROR,),
    )
    assert FlowResult.from_dict(error_output.to_dict()) == error_output

    with pytest.raises(InvalidStateError, match="same length"):
        FlowResult(
            results={},
            outputs=("value",),
            _output_kinds=(
                _FlowNodeInputKind.OUTCOME,
                _FlowNodeInputKind.ERROR,
            ),
        )

    with pytest.raises(SerDesError, match="different lengths"):
        FlowResult.from_dict(
            {
                "results": {},
                "outputs": ["value"],
                "outputProjections": [],
            }
        )

    with pytest.raises(SerDesError, match="invalid output projection"):
        FlowResult.from_dict(
            {
                "results": {},
                "outputs": ["value"],
                "outputProjections": ["UNKNOWN"],
            }
        )


def test_flow_node_result_is_rejected_outside_node_execution():
    captured = {}

    @durable_dag
    def graph():
        captured["source"] = node(return_name(), name="source")

    _evaluate_definition(graph())

    with pytest.raises(InvalidStateError, match="while a flow node is executing"):
        _ = captured["source"].result

    context = DurableContext(
        execution_state=Mock(spec=ExecutionState),
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
        ),
    )
    with bind_current_context(context):
        with pytest.raises(InvalidStateError, match="while a flow node is executing"):
            _ = captured["source"].result


async def test_flow_serdes_reject_non_mapping_payloads():
    delegate = ExtendedTypeSerDes()
    extended_payload = await delegate.serialize("not-a-mapping")
    flow_payload = await _NodeExecutionSerDes().delegate.serialize("not-a-mapping")

    with pytest.raises(SerDesError, match="node result"):
        await _NodeExecutionSerDes().deserialize(flow_payload)
    with pytest.raises(SerDesError, match="dependency resolution"):
        await _PersistedDependencyResolutionSerDes().deserialize(extended_payload)
    with pytest.raises(SerDesError, match="flow result"):
        await _FlowResultSerDes().deserialize(flow_payload)


async def test_flow_serdes_preserve_batch_result_outcomes():
    batch_result = BatchResult(
        all=[
            BatchItem(
                index=0,
                status=BatchItemStatus.SUCCEEDED,
                result="value",
            )
        ],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )
    node_result = FlowNodeResult.succeeded(batch_result)

    node_serdes = _NodeExecutionSerDes()
    restored_execution = await node_serdes.deserialize(
        await node_serdes.serialize(_NodeExecution(result=node_result))
    )
    assert isinstance(restored_execution.result.outcome, BatchResult)
    assert restored_execution.result.outcome == batch_result

    flow_serdes = _FlowResultSerDes()
    restored_flow = await flow_serdes.deserialize(
        await flow_serdes.serialize(
            FlowResult(
                results={"source": node_result},
                outputs=(batch_result, node_result),
                _output_kinds=(
                    _FlowNodeInputKind.OUTCOME,
                    _FlowNodeInputKind.RESULT,
                ),
            )
        )
    )
    assert isinstance(restored_flow.results["source"].outcome, BatchResult)
    assert restored_flow.results["source"].outcome == batch_result
    assert isinstance(restored_flow.outputs[0], BatchResult)
    projected_result = cast(
        "FlowNodeResult[BatchResult[str]]", restored_flow.outputs[1]
    )
    assert isinstance(projected_result.outcome, BatchResult)
    assert projected_result.outcome == batch_result


async def test_flow_serdes_preserve_flow_owned_outcomes_and_user_dicts():
    error = ErrorObject.from_message("failed")
    failed = FlowNodeResult.failed(error)
    nested_flow = FlowResult(
        results={"failed": failed},
        outputs=(failed, error),
        _output_kinds=(
            _FlowNodeInputKind.RESULT,
            _FlowNodeInputKind.ERROR,
        ),
    )
    user_dict = {
        "__async_durable_execution_flow_value__": 1,
        "kind": "FLOW_RESULT",
        "value": {"user": "data"},
    }
    outcome = {
        "flow": nested_flow,
        "result": failed,
        "error": error,
        "tuple": ("value",),
        "user_dict": user_dict,
    }
    serdes = _NodeExecutionSerDes()

    restored = await serdes.deserialize(
        await serdes.serialize(_NodeExecution(result=FlowNodeResult.succeeded(outcome)))
    )

    restored_outcome = cast("dict[str, Any]", restored.result.outcome)
    assert isinstance(restored_outcome["flow"], FlowResult)
    assert isinstance(restored_outcome["result"], FlowNodeResult)
    assert isinstance(restored_outcome["error"], ErrorObject)
    assert restored_outcome["flow"] == nested_flow
    assert restored_outcome["result"] == failed
    assert restored_outcome["error"] == error
    assert restored_outcome["tuple"] == ("value",)
    assert restored_outcome["user_dict"] == user_dict


def test_flow_value_encoding_rejects_circular_containers():
    circular = []
    circular.append(circular)

    with pytest.raises(SerDesError, match="Circular references"):
        _encode_flow_value(circular)


@pytest.mark.parametrize(
    ("value", "match"),
    [
        (
            {
                "__async_durable_execution_flow_value__": True,
                "kind": "FLOW_RESULT",
                "value": None,
            },
            "invalid envelope",
        ),
        (
            {
                "__async_durable_execution_flow_value__": 2,
                "kind": "FLOW_RESULT",
                "value": None,
            },
            "invalid envelope",
        ),
        (
            {"__async_durable_execution_flow_value__": 1},
            "invalid kind or payload",
        ),
        (
            {
                "__async_durable_execution_flow_value__": 1,
                "kind": "UNKNOWN",
                "value": None,
            },
            "invalid kind or payload",
        ),
        (
            {
                "__async_durable_execution_flow_value__": 1,
                "kind": "ESCAPED_DICT",
                "value": [],
            },
            "must be a mapping",
        ),
        (
            {
                "__async_durable_execution_flow_value__": 1,
                "kind": "FLOW_RESULT",
                "value": "not-a-mapping",
            },
            "must contain a mapping",
        ),
    ],
)
def test_flow_value_decoding_rejects_malformed_envelopes(value, match):
    with pytest.raises(SerDesError, match=match):
        _decode_flow_value(value)


def test_durable_dag_supports_class_and_static_method_decorator_orders():
    class Definitions:
        @classmethod
        @durable_dag
        def class_inside(cls, value: str):
            return value

        @durable_dag
        @classmethod
        def class_outside(cls, value: str):
            return value

        @staticmethod
        @durable_dag
        def static_inside(value: str):
            return value

        @durable_dag
        @staticmethod
        def static_outside(value: str):
            return value

    for bound in (
        Definitions.class_inside("a"),
        Definitions.class_outside("b"),
        Definitions.static_inside("c"),
        Definitions.static_outside("d"),
    ):
        assert getattr(bound, "_durable_dag_definition")


def test_durable_node_supports_class_and_static_method_decorator_orders():
    class Nodes:
        @classmethod
        @durable_node
        async def class_inside(cls, value: str):
            return value

        @durable_node
        @classmethod
        async def class_outside(cls, value: str):
            return value

        @staticmethod
        @durable_node
        async def static_inside(value: str):
            return value

        @durable_node
        @staticmethod
        async def static_outside(value: str):
            return value

    for bound in (
        Nodes.class_inside("a"),
        Nodes.class_outside("b"),
        Nodes.static_inside("c"),
        Nodes.static_outside("d"),
    ):
        assert getattr(bound, "_durable_node_callable")


def test_definition_returning_awaitable_is_rejected_and_closed():
    @durable_dag
    def invalid_graph():
        async def asynchronous_definition():
            return None

        return asynchronous_definition()

    with pytest.raises(FlowDefinitionError, match="synchronously"):
        _evaluate_definition(invalid_graph())


def test_definition_returning_noncoroutine_awaitable_is_rejected():
    class CustomAwaitable:
        def __await__(self):
            yield

    @durable_dag
    def invalid_graph():
        return CustomAwaitable()

    with pytest.raises(FlowDefinitionError, match="synchronously"):
        _evaluate_definition(invalid_graph())


def test_dependency_expression_rejects_empty_invalid_and_foreign_targets():
    captured = {}

    @durable_dag
    def first_graph():
        captured["foreign"] = node(return_name(), name="foreign")

    _evaluate_definition(first_graph())

    @durable_dag
    def empty_target_graph():
        source = node(return_name(), name="source")
        source >> ()

    with pytest.raises(FlowDefinitionError, match="at least one"):
        _evaluate_definition(empty_target_graph())

    @durable_dag
    def invalid_target_graph():
        source = node(return_name(), name="source")
        source.succeeded >> ("invalid",)

    with pytest.raises(FlowDefinitionError, match="FlowNode"):
        _evaluate_definition(invalid_target_graph())

    @durable_dag
    def foreign_target_graph():
        source = node(return_name(), name="source")
        source >> captured["foreign"]

    with pytest.raises(InvalidStateError, match="different flow"):
        _evaluate_definition(foreign_target_graph())


def test_nodes_and_expressions_cannot_cross_definition_boundaries():
    captured = {}

    @durable_dag
    def first_graph():
        source = node(return_name(), name="source")
        captured["source"] = source
        captured["expression"] = source.succeeded
        return source.outcome

    _evaluate_definition(first_graph())

    @durable_dag
    def mixed_expression_graph():
        local = node(return_name(), name="local")
        local.succeeded & captured["expression"]

    with pytest.raises(InvalidStateError, match="different flow"):
        _evaluate_definition(mixed_expression_graph())

    @durable_dag
    def foreign_output_graph():
        node(return_name(), name="local")
        return captured["source"].outcome

    with pytest.raises(InvalidStateError, match="outputs"):
        _evaluate_definition(foreign_output_graph())

    with pytest.raises(InvalidStateError, match="only be used"):
        captured["expression"] >> captured["source"]


def test_frozen_builder_rejects_late_mutation():
    captured = {}

    @durable_dag
    def graph():
        source = node(return_name(), name="source")
        captured["source"] = source
        return source.outcome

    _evaluate_definition(graph())
    source = captured["source"]
    builder = source._builder

    with pytest.raises(InvalidStateError, match="add nodes"):
        builder.add_node(return_name(), "late")
    with pytest.raises(InvalidStateError, match="add dependencies"):
        builder.add_dependency(source, source.succeeded)


def test_validation_rejects_noncallable_and_unknown_dependency():
    @durable_dag
    def noncallable_graph():
        node(None, name="invalid")

    with pytest.raises(FlowDefinitionError, match="callable"):
        _evaluate_definition(noncallable_graph())

    @durable_dag
    def unknown_dependency_graph():
        source = node(return_name(), name="source")
        target = node(return_name(), name="target")
        ghost = FlowNode(source._builder, 99, return_name(), "ghost")
        ghost >> target

    with pytest.raises(FlowDefinitionError, match="unknown dependency"):
        _evaluate_definition(unknown_dependency_graph())


def test_builder_validation_rejects_tampered_node_callable():
    builder = _FlowBuilder()
    flow_node = builder.add_node(return_name(), "invalid")
    flow_node._func = None

    with pytest.raises(FlowDefinitionError, match="@durable_node"):
        builder.freeze(None)


def test_validation_rejects_dependency_owned_by_another_builder():
    captured = {}

    @durable_dag
    def first_graph():
        source = node(return_name(), name="source")
        captured["expression"] = source.succeeded

    _evaluate_definition(first_graph())

    @durable_dag
    def second_graph():
        target = node(return_name(), name="target")
        target._dependency = captured["expression"]

    with pytest.raises(InvalidStateError, match="current definition"):
        _evaluate_definition(second_graph())


def test_coerce_expression_rejects_invalid_values():
    with pytest.raises(FlowDefinitionError, match="Dependencies"):
        _coerce_expression("invalid")


@pytest.mark.parametrize(
    "data",
    [
        "{",
        "[]",
        json.dumps({"__async_durable_execution_error__": 2}),
        json.dumps(
            {
                "__async_durable_execution_error__": 1,
                "exception_type": (
                    "async_durable_execution.exceptions.InvocationError"
                ),
                "payload": {},
            }
        ),
    ],
)
def test_sdk_error_data_rejects_invalid_envelopes(data):
    assert _decode_sdk_error_data(data, InvocationError) == (False, None)


@pytest.mark.parametrize(
    ("error_type", "data", "expected_type"),
    [
        (
            "CheckpointError",
            _encode_sdk_error_data(InvocationError),
            InvocationError,
        ),
        (
            "ExecutionError",
            _encode_sdk_error_data(ExecutionError),
            ExecutionError,
        ),
        (
            "SerDesError",
            _encode_sdk_error_data(SerDesError),
            ExecutionError,
        ),
        (
            "CallbackError",
            _encode_sdk_error_data(CallbackError),
            ExecutionError,
        ),
        (
            "CallbackError",
            json.dumps(
                {
                    "__async_durable_execution_error__": 1,
                    "exception_type": (
                        "async_durable_execution.exceptions.CallbackError"
                    ),
                    "payload": None,
                }
            ),
            ExecutionError,
        ),
        ("ExecutionError", None, type(None)),
        (
            "ExecutionError",
            _encode_sdk_error_data(InvocationError),
            InvocationError,
        ),
        ("UnknownUserError", None, type(None)),
    ],
)
def test_callable_runtime_error_control_classification(
    error_type,
    data,
    expected_type,
):
    error = CallableRuntimeError(
        message="failure",
        error_type=error_type,
        data=data,
        stack_trace=None,
    )
    classified = _find_control_error(error)

    if expected_type is type(None):
        assert classified is None
    else:
        assert isinstance(classified, expected_type)


@pytest.mark.parametrize("retryable", [False, True])
def test_replayed_custom_invocation_error_preserves_retry_behavior(retryable):
    source = _CustomInvocationError("custom failure", retryable=retryable)
    data = _encode_sdk_control_error_data(source)
    assert data is not None

    classified = _find_control_error(
        CallableRuntimeError(
            message=str(source),
            error_type=type(source).__name__,
            data=data,
            stack_trace=None,
        )
    )

    assert isinstance(classified, InvocationError)
    assert classified.is_retryable() is retryable
    assert classified.termination_reason is TerminationReason.STEP_INTERRUPTED
    assert _sdk_error_type_name(classified) == "_CustomInvocationError"
    assert classified.build_logger_extras() == {}

    reencoded = _encode_sdk_control_error_data(classified)
    assert reencoded is not None
    reclassified = _find_control_error(
        CallableRuntimeError(
            message=str(classified),
            error_type=type(classified).__name__,
            data=reencoded,
            stack_trace=None,
        )
    )
    assert isinstance(reclassified, InvocationError)
    assert reclassified.is_retryable() is retryable
    assert _sdk_error_type_name(reclassified) == "_CustomInvocationError"


async def test_replayed_non_retryable_boto_error_remains_non_retryable():
    source = BotoClientError(
        "KMS access denied",
        error_category=DurableApiErrorCategory.EXECUTION,
        error={
            "Code": "KMSAccessDeniedException",
            "Message": "KMS access denied",
        },
        response_metadata={
            "RequestId": "request-id",
            "HTTPStatusCode": 502,
        },
    )
    data = _encode_sdk_control_error_data(source)
    assert data is not None

    classified = _find_control_error(
        CallableRuntimeError(
            message=str(source),
            error_type=type(source).__name__,
            data=data,
            stack_trace=None,
        )
    )

    assert isinstance(classified, InvocationError)
    assert not classified.is_retryable()
    assert classified.build_logger_extras() == {
        "Error": {
            "Code": "KMSAccessDeniedException",
            "Message": "KMS access denied",
        },
        "ResponseMetadata": {
            "RequestId": "request-id",
            "HTTPStatusCode": 502,
        },
    }

    output = await handle_user_function_exception(Mock(), classified)
    assert output.status is InvocationStatus.FAILED
    assert output.error is not None
    assert output.error.type == "BotoClientError"


@pytest.mark.parametrize(
    ("source", "expected_reason"),
    [
        (
            _CustomExecutionError(
                "custom execution failure",
                TerminationReason.NON_DETERMINISTIC_EXECUTION,
            ),
            TerminationReason.NON_DETERMINISTIC_EXECUTION,
        ),
        (
            _CustomSerDesError("custom serialization failure"),
            TerminationReason.SERIALIZATION_ERROR,
        ),
    ],
)
async def test_replayed_custom_execution_control_error_preserves_category(
    source,
    expected_reason,
):
    data = _encode_sdk_control_error_data(source)
    assert data is not None

    classified = _find_control_error(
        CallableRuntimeError(
            message=str(source),
            error_type=type(source).__name__,
            data=data,
            stack_trace=None,
        )
    )

    assert isinstance(classified, ExecutionError)
    assert classified.termination_reason is expected_reason
    assert _sdk_error_type_name(classified) == type(source).__name__

    reencoded = _encode_sdk_control_error_data(classified)
    assert reencoded is not None
    reclassified = _find_control_error(
        CallableRuntimeError(
            message=str(classified),
            error_type=type(classified).__name__,
            data=reencoded,
            stack_trace=None,
        )
    )
    assert isinstance(reclassified, ExecutionError)
    assert reclassified.termination_reason is expected_reason
    assert _sdk_error_type_name(reclassified) == type(source).__name__

    output = await handle_user_function_exception(Mock(), reclassified)
    assert output.status is InvocationStatus.FAILED
    assert output.error is not None
    assert output.error.type == type(source).__name__


def test_replayed_legacy_execution_error_preserves_termination_reason():
    classified = _find_control_error(
        CallableRuntimeError(
            message="legacy execution failure",
            error_type="CustomLegacyExecutionError",
            data=_encode_sdk_error_data(
                ExecutionError,
                TerminationReason.NON_DETERMINISTIC_EXECUTION.value,
            ),
            stack_trace=None,
        )
    )

    assert isinstance(classified, ExecutionError)
    assert (
        classified.termination_reason is TerminationReason.NON_DETERMINISTIC_EXECUTION
    )
    assert _sdk_error_type_name(classified) == "CustomLegacyExecutionError"


@pytest.mark.parametrize(
    "payload",
    [
        "{",
        json.dumps(
            {
                "version": 1,
                "error_type": 42,
                "termination_reason": "INVALID",
            }
        ),
    ],
)
def test_invalid_execution_error_payload_uses_safe_defaults(payload):
    classified = _find_control_error(
        CallableRuntimeError(
            message="execution failure",
            error_type="FallbackExecutionError",
            data=_encode_sdk_error_data(ExecutionError, payload),
            stack_trace=None,
        )
    )

    assert isinstance(classified, ExecutionError)
    assert classified.termination_reason is TerminationReason.EXECUTION_ERROR
    assert _sdk_error_type_name(classified) == "FallbackExecutionError"


@pytest.mark.skipif(
    not hasattr(builtins, "ExceptionGroup"),
    reason="ExceptionGroup requires Python 3.11 or newer",
)
def test_control_error_is_found_in_nested_exception_group():
    group_type = getattr(builtins, "ExceptionGroup")
    source = _CustomExecutionError(
        "nested control failure",
        TerminationReason.NON_DETERMINISTIC_EXECUTION,
    )
    nested = group_type("nested", [ValueError("user failure"), source])
    outer = group_type("outer", [RuntimeError("other failure"), nested])

    assert _find_control_error(outer) is source


def test_control_error_search_handles_cyclic_cause_chain():
    error = RuntimeError("cyclic")
    error.__cause__ = error

    assert _find_control_error(error) is None


def test_invocation_error_payload_drops_unserializable_boto_diagnostics():
    source = BotoClientError(
        "invalid diagnostics",
        error_category=DurableApiErrorCategory.EXECUTION,
        error={
            "Code": "KMSAccessDeniedException",
            "Message": cast("Any", object()),
        },
        response_metadata={
            "HTTPHeaders": cast("Any", object()),
        },
    )
    data = _encode_sdk_control_error_data(source)
    assert data is not None

    classified = _find_control_error(
        CallableRuntimeError(
            message=str(source),
            error_type=type(source).__name__,
            data=data,
            stack_trace=None,
        )
    )

    assert isinstance(classified, InvocationError)
    assert not classified.is_retryable()
    assert classified.build_logger_extras() == {}


@pytest.mark.parametrize(
    "payload",
    [
        "{",
        json.dumps(
            {
                "version": 1,
                "error_type": "",
                "retryable": "yes",
                "termination_reason": "UNKNOWN",
                "error_category": "UNKNOWN",
                "error": [],
                "response_metadata": [],
            }
        ),
    ],
)
def test_invalid_invocation_error_payload_fails_closed(payload):
    classified = _find_control_error(
        CallableRuntimeError(
            message="legacy failure",
            error_type=None,
            data=_encode_sdk_error_data(InvocationError, payload),
            stack_trace=None,
        )
    )

    assert isinstance(classified, InvocationError)
    assert not classified.is_retryable()
    assert classified.termination_reason is TerminationReason.INVOCATION_ERROR
    assert _sdk_error_type_name(classified) == "InvocationError"
    assert classified.build_logger_extras() == {}


def test_callable_runtime_error_preserves_original_error_details():
    from async_durable_execution.extension.flow import _callable_error_object

    error = CallableRuntimeError(
        message="failure",
        error_type="ValueError",
        data="details",
        stack_trace=["line"],
    )

    assert _callable_error_object(error) == ErrorObject(
        message="failure",
        type="ValueError",
        data="details",
        stack_trace=["line"],
    )


def test_flow_node_repr_and_flat_expression_construction():
    captured = {}

    @durable_dag
    def graph():
        a = node(return_name(), name="A")
        b = node(return_name(), name="B")
        c = node(return_name(), name="C")
        d = node(return_name(), name="D")
        (a | b | c) >> d
        captured["a"] = a
        captured["d"] = d
        return d.outcome

    frozen = _evaluate_definition(graph())

    assert repr(captured["a"]) == "FlowNode(name='A')"
    assert [flow_node.name for flow_node in frozen.topological_nodes] == [
        "A",
        "B",
        "C",
        "D",
    ]
    assert [flow_node.name for flow_node in frozen.execution_nodes] == [
        "A",
        "B",
        "C",
        "D",
    ]
    assert len(captured["d"]._dependency.children) == 3


def test_flow_builder_defensive_cycle_error():
    builder = _FlowBuilder()
    first = builder.add_node(return_name(), "first")
    second = builder.add_node(return_name(), "second")
    third = builder.add_node(return_name(), "third")
    outside = builder.add_node(return_name(), "outside")
    adjacency = {
        first: [second, third],
        second: [],
        third: [second],
        outside: [],
    }

    with pytest.raises(FlowDefinitionError, match="failed to identify"):
        builder._find_cycle(adjacency, {first, second, third})


def test_composite_expression_default_state_and_all_unmatched():
    captured = {}

    @durable_dag
    def graph():
        a = node(return_name(), name="A")
        b = node(return_name(), name="B")
        c = node(return_name(), name="C")
        d = node(return_name(), name="D")
        (a & b) >> c
        (a | b) >> d
        captured.update(a=a, b=b, c=c, d=d)
        return c.outcome, d.outcome

    _evaluate_definition(graph())
    results = {
        captured["a"]: FlowNodeResult.failed(ErrorObject.from_message("failed")),
        captured["b"]: FlowNodeResult.failed(ErrorObject.from_message("failed")),
    }

    assert captured["c"]._dependency.evaluate(results).status.value == "UNMATCHED"
    assert captured["d"]._dependency.evaluate(results).status.value == "UNMATCHED"
    assert captured["d"]._dependency.evaluate({}).status.value == "PENDING"


def test_flow_node_input_resolution_and_container_helpers():
    builder = _FlowBuilder()
    source = builder.add_node(return_name(), "source")
    outcome_input = _FlowNodeInput(source, _FlowNodeInputKind.OUTCOME)
    error_input = _FlowNodeInput(source, _FlowNodeInputKind.ERROR)
    result_input = _FlowNodeInput(source, _FlowNodeInputKind.RESULT)
    error = ErrorObject.from_message("failed")
    success = FlowNodeResult.succeeded("value")

    assert outcome_input.resolve({source: success}) == "value"
    assert error_input.resolve({source: FlowNodeResult.failed(error)}) is error
    assert result_input.resolve({source: success}) is success

    with pytest.raises(InvalidStateError, match="not available"):
        outcome_input.resolve({})
    with pytest.raises(InvalidStateError, match="expected SUCCEEDED"):
        outcome_input.resolve({source: FlowNodeResult.failed(error)})
    with pytest.raises(InvalidStateError, match="has no error"):
        error_input.resolve(
            {
                source: FlowNodeResult(
                    status=FlowNodeStatus.FAILED,
                    error=None,
                )
            }
        )

    unchanged_list = ["value"]
    unchanged_tuple = ("value",)
    unchanged_dict = {"key": "value"}
    assert _resolve_flow_node_inputs(unchanged_list, {}) is unchanged_list
    assert _resolve_flow_node_inputs(unchanged_tuple, {}) is unchanged_tuple
    assert _resolve_flow_node_inputs(unchanged_dict, {}) is unchanged_dict

    class Pair(NamedTuple):
        first: object
        second: object

    resolved_pair = _resolve_flow_node_inputs(
        Pair(outcome_input, "other"),
        {source: FlowNodeResult.succeeded("value")},
    )
    assert resolved_pair == Pair("value", "other")
    assert isinstance(resolved_pair, Pair)

    recursive: list[object] = []
    recursive.append(recursive)
    with pytest.raises(FlowDefinitionError, match="recursive containers"):
        _flow_node_inputs(recursive)

    @dataclass
    class Payload:
        value: object

    class SlottedPayload:
        __slots__ = "value"

        def __init__(self, value: object) -> None:
            self.value = value

    class ObjectPayload:
        def __init__(self, value: object) -> None:
            self.value = value

    class Attribute:
        name = "value"

    class AttrsPayload:
        __attrs_attrs__ = (Attribute(),)

        def __init__(self, value: object) -> None:
            self.value = value

    for unsupported in (
        {outcome_input},
        frozenset({outcome_input}),
        Payload([outcome_input]),
        AttrsPayload((outcome_input,)),
        ObjectPayload(outcome_input),
        SlottedPayload({"value": outcome_input}),
        deque([outcome_input]),
    ):
        with pytest.raises(
            FlowDefinitionError,
            match="unsupported container type",
        ):
            _flow_node_inputs(unsupported)

    with pytest.raises(FlowDefinitionError, match="cannot use iterators"):
        _flow_node_inputs(iter([outcome_input]))

    recursive_payload = ObjectPayload(None)
    recursive_payload.value = recursive_payload
    assert _flow_node_inputs(recursive_payload) == ()


def test_node_inputs_reject_foreign_nodes_and_form_all_dependencies():
    captured = {}

    @durable_node
    async def consume(*values: object) -> None:
        _ = values

    @durable_dag
    def first_graph():
        captured["foreign"] = node(return_name(), name="foreign")

    _evaluate_definition(first_graph())

    @durable_dag
    def invalid_graph():
        node(consume(captured["foreign"].outcome), name="target")

    with pytest.raises(InvalidStateError, match="current flow definition"):
        _evaluate_definition(invalid_graph())

    @durable_dag
    def all_graph():
        first = node(return_name(), name="first")
        second = node(return_name(), name="second")
        captured["target"] = node(
            consume(first.outcome, second.outcome),
            name="target",
        )

    _evaluate_definition(all_graph())
    dependency = captured["target"]._dependency
    assert [leaf.node.name for leaf in dependency.leaves()] == ["first", "second"]


def test_cycle_search_skips_nonremaining_targets_and_backtracks():
    builder = _FlowBuilder()
    a = builder.add_node(return_name(), "A")
    branch = builder.add_node(return_name(), "branch")
    b = builder.add_node(return_name(), "B")
    outside = builder.add_node(return_name(), "outside")
    adjacency = {
        a: [outside, branch, b],
        branch: [],
        b: [a],
        outside: [],
    }

    cycle = builder._find_cycle(adjacency, {a, branch, b})

    assert [flow_node.name for flow_node in cycle] == ["A", "B", "A"]


async def test_dependency_resolution_propagates_unclassified_task_error():
    captured = {}

    @durable_dag
    def graph():
        a = node(return_name(), name="A")
        b = node(return_name(), name="B")
        a >> b
        captured.update(a=a, b=b)
        return b.outcome

    _evaluate_definition(graph())

    async def fail():
        msg = "unexpected dependency failure"
        raise ValueError(msg)

    task = asyncio.create_task(fail())
    with pytest.raises(ValueError, match="unexpected dependency"):
        await _resolve_dependency_expression(
            captured["b"],
            captured["b"]._dependency,
            {captured["a"]: task},
            {},
        )


async def test_all_dependency_resolution_returns_unmatched_child_results():
    captured = {}

    @durable_dag
    def graph():
        first = node(return_name(), name="first")
        second = node(return_name(), name="second")
        target = node(return_name(), name="target")
        (first & second) >> target
        captured.update(first=first, second=second, target=target)

    _evaluate_definition(graph())

    async def execution(result: FlowNodeResult[object]) -> _NodeExecution:
        return _NodeExecution(result=result)

    tasks = {
        captured["first"]: asyncio.create_task(
            execution(FlowNodeResult.failed(ErrorObject.from_message("failed")))
        ),
        captured["second"]: asyncio.create_task(
            execution(FlowNodeResult.succeeded("ok"))
        ),
    }
    resolution = await _resolve_dependency_expression(
        captured["target"],
        captured["target"]._dependency,
        tasks,
        {},
    )

    assert not resolution.matched
    assert set(resolution.results) == {captured["first"], captured["second"]}


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (_FlowControlSignal(ValueError("control")), _FlowControlSignal),
        (
            CallableRuntimeError(
                message="serialized control",
                error_type="ExecutionError",
                data=_encode_sdk_error_data(ExecutionError),
                stack_trace=None,
            ),
            _FlowControlSignal,
        ),
        (ValueError("ordinary"), ValueError),
    ],
)
async def test_await_resolver_resolution_propagates_task_errors(error, expected):
    captured = {}

    @durable_dag
    def graph():
        source = node(return_name(), name="source")
        target = node(return_name(), name="target")
        source >> target
        captured["expression"] = target._dependency

    _evaluate_definition(graph())

    async def fail():
        raise error

    task = asyncio.create_task(fail())
    with pytest.raises(expected) as raised:
        await _await_resolver_resolution(task, captured["expression"], {})

    if isinstance(error, _FlowControlSignal):
        assert raised.value is error
    elif isinstance(error, CallableRuntimeError):
        assert isinstance(raised.value.error, ExecutionError)


def test_raise_task_error_preserves_control_and_ordinary_failures():
    signal = _FlowControlSignal(ValueError("control"))
    with pytest.raises(_FlowControlSignal) as raised:
        _raise_task_error(signal)
    assert raised.value is signal

    with pytest.raises(_FlowControlSignal) as raised:
        _raise_task_error(ExecutionError("sdk"))
    assert isinstance(raised.value.error, ExecutionError)

    ordinary = ValueError("ordinary")
    with pytest.raises(ValueError) as raised:
        _raise_task_error(ordinary)
    assert raised.value is ordinary


async def test_any_resolution_propagates_child_control_signal():
    captured = {}
    blocker = asyncio.Event()

    @durable_dag
    def graph():
        first = node(return_name(), name="first")
        second = node(return_name(), name="second")
        target = node(return_name(), name="target")
        (first | second) >> target
        captured.update(first=first, second=second, target=target)

    _evaluate_definition(graph())

    signal = _FlowControlSignal(ValueError("control"))

    async def fail():
        raise signal

    async def wait_forever():
        await blocker.wait()
        return _NodeExecution(FlowNodeResult.succeeded("late"))

    tasks = {
        captured["first"]: asyncio.create_task(fail()),
        captured["second"]: asyncio.create_task(wait_forever()),
    }
    with pytest.raises(_FlowControlSignal) as raised:
        await _resolve_any_expression(
            captured["target"],
            captured["target"]._dependency,
            tasks,
            {},
        )
    assert raised.value is signal


def _any_resolution_nodes():
    captured = {}

    @durable_dag
    def graph():
        first = node(return_name(), name="first")
        second = node(return_name(), name="second")
        target = node(return_name(), name="target")
        (first | second) >> target
        captured.update(first=first, second=second, target=target)

    _evaluate_definition(graph())
    return captured["first"], captured["second"], captured["target"]


async def test_any_resolution_preserves_reverse_completion_order(monkeypatch):
    first, second, target = _any_resolution_nodes()
    expression = cast(Any, target._dependency)
    release_first = asyncio.Event()
    first_finished = asyncio.Event()

    async def resolve_child(_target, child, _tasks, _resolver_tasks):
        if child is expression.children[0]:
            await release_first.wait()
            first_finished.set()
            return _DependencyResolution(
                matched=True,
                results={first: FlowNodeResult.succeeded("declaration-first")},
            )
        release_first.set()
        return _DependencyResolution(
            matched=True,
            results={second: FlowNodeResult.succeeded("completion-first")},
        )

    def create_normal_task(coro_factory):
        return asyncio.create_task(coro_factory())

    monkeypatch.setattr(
        "async_durable_execution.extension.flow._resolve_dependency_expression",
        resolve_child,
    )
    monkeypatch.setattr(
        "async_durable_execution.extension.flow.create_eager_task",
        create_normal_task,
    )

    resolution = await _resolve_any_expression(target, expression, {}, {})

    assert first_finished.is_set()
    assert resolution.selected_nodes == ("second",)


async def test_any_resolution_continues_after_suspended_candidate():
    first, second, target = _any_resolution_nodes()
    suspension = SuspendExecution("waiting for callback")

    async def suspend():
        raise suspension

    async def succeed():
        return _NodeExecution(FlowNodeResult.succeeded("winner"))

    resolution = await _resolve_any_expression(
        target,
        target._dependency,
        {
            first: asyncio.create_task(suspend()),
            second: asyncio.create_task(succeed()),
        },
        {},
    )

    assert resolution.matched
    assert resolution.selected_nodes == ("second",)


async def test_any_resolution_suspends_when_no_candidate_can_match():
    first, second, target = _any_resolution_nodes()
    suspension = SuspendExecution("waiting for callback")

    async def suspend():
        raise suspension

    async def fail():
        return _NodeExecution(FlowNodeResult.failed(ErrorObject.from_message("failed")))

    with pytest.raises(SuspendExecution) as raised:
        await _resolve_any_expression(
            target,
            target._dependency,
            {
                first: asyncio.create_task(suspend()),
                second: asyncio.create_task(fail()),
            },
            {},
        )
    assert raised.value is suspension


async def test_any_resolution_uses_earliest_timed_suspension():
    first, second, target = _any_resolution_nodes()
    later = TimedSuspendExecution("later", 20)
    earlier = TimedSuspendExecution("earlier", 10)

    async def suspend(error: TimedSuspendExecution):
        raise error

    with pytest.raises(TimedSuspendExecution) as raised:
        await _resolve_any_expression(
            target,
            target._dependency,
            {
                first: asyncio.create_task(suspend(later)),
                second: asyncio.create_task(suspend(earlier)),
            },
            {},
        )
    assert raised.value is earlier


def test_flow_node_context_reuses_existing_step_counter():
    state = Mock(spec=ExecutionState)
    fresh_context = DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id="parent",
        ),
    )
    fresh_flow_context = _flow_node_context(fresh_context, frozenset(), {})
    assert "step_counter" not in fresh_flow_context.__dict__

    context = DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
            parent_id="parent",
        ),
        step_id_prefix="prefix",
    )
    counter = context.step_counter

    flow_context = _flow_node_context(context, frozenset(), {})

    assert flow_context.step_counter is counter


async def test_execute_flow_wraps_unclassified_child_error(monkeypatch):
    @durable_dag
    def graph():
        return node(return_name(), name="A").outcome

    frozen = _evaluate_definition(graph())

    async def fail():
        msg = "unexpected child failure"
        raise ValueError(msg)

    def fake_child(*args, **kwargs):
        return asyncio.create_task(fail())

    monkeypatch.setattr(
        "async_durable_execution.extension.flow.run_in_child_context",
        fake_child,
    )

    with pytest.raises(_FlowControlSignal) as raised:
        await _execute_flow(frozen)
    assert isinstance(raised.value.error, ValueError)


async def test_execute_flow_prioritizes_resolver_error_over_node_suspension(
    monkeypatch,
):
    @durable_dag
    def graph():
        first = node(return_name(), name="first")
        second = node(return_name(), name="second")
        target = node(return_name(), name="target")
        (first | second) >> target
        return target.outcome

    frozen = _evaluate_definition(graph())
    suspension = TimedSuspendExecution("node suspended", 100)
    control_error = _FlowControlSignal(InvocationError("checkpoint failed"))

    async def succeed():
        return _NodeExecution(FlowNodeResult.succeeded("ok"))

    async def fail(error: BaseException):
        raise error

    def fake_child(func, *, name, **kwargs):
        _ = func, kwargs
        if name == "first":
            return asyncio.create_task(fail(suspension))
        if name.startswith("flow-any-resolution-"):
            return asyncio.create_task(fail(control_error))
        return asyncio.create_task(succeed())

    monkeypatch.setattr(
        "async_durable_execution.extension.flow.run_in_child_context",
        fake_child,
    )

    with pytest.raises(_FlowControlSignal) as raised:
        await _execute_flow(frozen)
    assert raised.value is control_error


async def test_execute_flow_uses_earliest_timed_suspension(monkeypatch):
    @durable_dag
    def graph():
        first = node(return_name(), name="first")
        second = node(return_name(), name="second")
        third = node(return_name(), name="third")
        return first.outcome, second.outcome, third.outcome

    frozen = _evaluate_definition(graph())
    indefinite = SuspendExecution("indefinite")
    later = TimedSuspendExecution("later", 20)
    earlier = TimedSuspendExecution("earlier", 10)
    errors = {
        "first": indefinite,
        "second": later,
        "third": earlier,
    }

    async def fail(error: BaseException):
        raise error

    def fake_child(func, *, name, **kwargs):
        _ = func, kwargs
        return asyncio.create_task(fail(errors[name]))

    monkeypatch.setattr(
        "async_durable_execution.extension.flow.run_in_child_context",
        fake_child,
    )

    with pytest.raises(TimedSuspendExecution) as raised:
        await _execute_flow(frozen)
    assert raised.value is earlier


def test_collected_task_errors_use_indefinite_suspension_as_fallback():
    suspension = SuspendExecution("waiting for callback")

    with pytest.raises(SuspendExecution) as raised:
        _raise_collected_task_errors([suspension])
    assert raised.value is suspension


@pytest.mark.parametrize(
    ("resolver_error", "expected"),
    [
        (_FlowControlSignal(ValueError("control")), _FlowControlSignal),
        (ExecutionError("sdk control"), _FlowControlSignal),
        (ValueError("ordinary"), _FlowControlSignal),
        (_FatalFlowSignal("fatal"), _FatalFlowSignal),
    ],
)
async def test_execute_flow_propagates_resolver_task_errors(
    monkeypatch,
    resolver_error,
    expected,
):
    @durable_dag
    def graph():
        first = node(return_name(), name="first")
        second = node(return_name(), name="second")
        target = node(return_name(), name="target")
        (first | second) >> target
        return target.outcome

    frozen = _evaluate_definition(graph())

    async def succeed():
        return _NodeExecution(FlowNodeResult.succeeded("ok"))

    async def fail():
        raise resolver_error

    def fake_child(func, *, name, **kwargs):
        _ = func, kwargs
        coroutine = fail() if name.startswith("flow-any-resolution-") else succeed()
        return asyncio.create_task(coroutine)

    monkeypatch.setattr(
        "async_durable_execution.extension.flow.run_in_child_context",
        fake_child,
    )

    with pytest.raises(expected) as raised:
        await _execute_flow(frozen)

    if isinstance(resolver_error, (_FlowControlSignal, _FatalFlowSignal)):
        assert raised.value is resolver_error
    else:
        assert isinstance(raised.value.error, type(resolver_error))


@pytest.mark.parametrize(
    ("child_error", "expected_error"),
    [
        (
            CallableRuntimeError(
                message="serialized control",
                error_type="ExecutionError",
                data=_encode_sdk_error_data(ExecutionError),
                stack_trace=None,
            ),
            ExecutionError,
        ),
        (ValueError("ordinary failure"), ValueError),
    ],
)
async def test_flow_boundary_classifies_child_task_errors(
    monkeypatch,
    child_error,
    expected_error,
):
    @durable_dag
    def graph():
        return None

    async def fail():
        raise child_error

    def fake_child(*args, **kwargs):
        return asyncio.create_task(fail())

    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = "test"
    context = DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
        ),
    )
    monkeypatch.setattr(
        "async_durable_execution.extension.flow.run_in_child_context",
        fake_child,
    )

    with bind_current_context(context):
        flow_task = flow(graph())
        with pytest.raises(expected_error):
            await flow_task

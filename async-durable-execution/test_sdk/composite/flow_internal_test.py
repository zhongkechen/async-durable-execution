"""Focused tests for flow definition validation and result helpers."""

from __future__ import annotations

import asyncio
from typing import cast
from unittest.mock import Mock

import pytest

from async_durable_execution import (
    DurableContext,
    ErrorObject,
    FlowDefinitionError,
    FlowNodeContext,
    FlowNodeResult,
    FlowResult,
    InvalidStateError,
    durable_dag,
    flow,
    node,
)
from async_durable_execution.composite.flow import (
    FlowNode,
    _DependencyResolution,
    _FlowBuilder,
    _FlowControlSignal,
    _FlowResultSerDes,
    _NodeExecutionSerDes,
    _coerce_expression,
    _evaluate_definition,
    _execute_flow,
    _flow_node_context,
    _resolve_dependencies,
)
from async_durable_execution.context import bind_current_context
from async_durable_execution.exceptions import (
    CallableRuntimeError,
    ExecutionError,
    InvocationError,
    SerDesError,
)
from async_durable_execution.models import OperationIdentifier, OperationSubType
from async_durable_execution.serdes import ExtendedTypeSerDes
from async_durable_execution.state import ExecutionState


async def return_name(context: FlowNodeContext) -> str:
    return context.operation_name or ""


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


async def test_flow_serdes_reject_non_mapping_payloads():
    delegate = ExtendedTypeSerDes()
    payload = await delegate.serialize("not-a-mapping")

    with pytest.raises(SerDesError, match="node result"):
        await _NodeExecutionSerDes().deserialize(payload)
    with pytest.raises(SerDesError, match="flow result"):
        await _FlowResultSerDes().deserialize(payload)


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


def test_definition_returning_awaitable_is_rejected_and_closed():
    @durable_dag
    def invalid_graph():
        async def asynchronous_definition():
            return None

        return asynchronous_definition()

    with pytest.raises(FlowDefinitionError, match="synchronously"):
        _evaluate_definition(invalid_graph())


def test_dependency_expression_rejects_empty_invalid_and_foreign_targets():
    captured = {}

    @durable_dag
    def first_graph():
        captured["foreign"] = node(return_name, name="foreign")

    _evaluate_definition(first_graph())

    @durable_dag
    def empty_target_graph():
        source = node(return_name, name="source")
        source >> ()

    with pytest.raises(FlowDefinitionError, match="at least one"):
        _evaluate_definition(empty_target_graph())

    @durable_dag
    def invalid_target_graph():
        source = node(return_name, name="source")
        source.succeeded >> ("invalid",)

    with pytest.raises(FlowDefinitionError, match="FlowNode"):
        _evaluate_definition(invalid_target_graph())

    @durable_dag
    def foreign_target_graph():
        source = node(return_name, name="source")
        source >> captured["foreign"]

    with pytest.raises(InvalidStateError, match="different flow"):
        _evaluate_definition(foreign_target_graph())


def test_nodes_and_expressions_cannot_cross_definition_boundaries():
    captured = {}

    @durable_dag
    def first_graph():
        source = node(return_name, name="source")
        captured["source"] = source
        captured["expression"] = source.succeeded
        return source

    _evaluate_definition(first_graph())

    @durable_dag
    def mixed_expression_graph():
        local = node(return_name, name="local")
        local.succeeded & captured["expression"]

    with pytest.raises(InvalidStateError, match="different flow"):
        _evaluate_definition(mixed_expression_graph())

    @durable_dag
    def foreign_output_graph():
        node(return_name, name="local")
        return captured["source"]

    with pytest.raises(InvalidStateError, match="outputs"):
        _evaluate_definition(foreign_output_graph())

    with pytest.raises(InvalidStateError, match="only be used"):
        captured["expression"] >> captured["source"]


def test_frozen_builder_rejects_late_mutation():
    captured = {}

    @durable_dag
    def graph():
        source = node(return_name, name="source")
        captured["source"] = source
        return source

    _evaluate_definition(graph())
    source = captured["source"]
    builder = source._builder

    with pytest.raises(InvalidStateError, match="add nodes"):
        builder.add_node(return_name, "late")
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
        source = node(return_name, name="source")
        target = node(return_name, name="target")
        ghost = FlowNode(source._builder, 99, return_name, "ghost")
        ghost >> target

    with pytest.raises(FlowDefinitionError, match="unknown dependency"):
        _evaluate_definition(unknown_dependency_graph())


def test_validation_rejects_dependency_owned_by_another_builder():
    captured = {}

    @durable_dag
    def first_graph():
        source = node(return_name, name="source")
        captured["expression"] = source.succeeded

    _evaluate_definition(first_graph())

    @durable_dag
    def second_graph():
        target = node(return_name, name="target")
        target._dependency = captured["expression"]

    with pytest.raises(InvalidStateError, match="current definition"):
        _evaluate_definition(second_graph())


def test_coerce_expression_rejects_invalid_values():
    with pytest.raises(FlowDefinitionError, match="Dependencies"):
        _coerce_expression("invalid")


@pytest.mark.parametrize(
    ("error_type", "expected_type"),
    [
        ("CheckpointError", InvocationError),
        ("ExecutionError", ExecutionError),
        ("UnknownUserError", type(None)),
    ],
)
def test_callable_runtime_error_control_classification(error_type, expected_type):
    from async_durable_execution.composite.flow import _find_control_error

    error = CallableRuntimeError(
        message="failure",
        error_type=error_type,
        data=None,
        stack_trace=None,
    )
    classified = _find_control_error(error)

    if expected_type is type(None):
        assert classified is None
    else:
        assert isinstance(classified, expected_type)


def test_callable_runtime_error_preserves_original_error_details():
    from async_durable_execution.composite.flow import _callable_error_object

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
        a = node(return_name, name="A")
        b = node(return_name, name="B")
        c = node(return_name, name="C")
        d = node(return_name, name="D")
        (a | b | c) >> d
        captured["a"] = a
        captured["d"] = d
        return d

    frozen = _evaluate_definition(graph())

    assert repr(captured["a"]) == "FlowNode(name='A')"
    assert [flow_node.name for flow_node in frozen.topological_nodes] == [
        "A",
        "B",
        "C",
        "D",
    ]
    assert len(captured["d"]._dependency.children) == 3


def test_flow_builder_defensive_cycle_error():
    builder = _FlowBuilder()

    with pytest.raises(FlowDefinitionError, match="failed to identify"):
        builder._find_cycle({}, set())


def test_composite_expression_default_state_and_all_unmatched():
    captured = {}

    @durable_dag
    def graph():
        a = node(return_name, name="A")
        b = node(return_name, name="B")
        c = node(return_name, name="C")
        (a & b) >> c
        captured.update(a=a, b=b, c=c)
        return c

    _evaluate_definition(graph())
    expression = captured["c"]._dependency

    evaluation = expression.evaluate(
        {
            captured["a"]: FlowNodeResult.failed(ErrorObject.from_message("failed")),
            captured["b"]: FlowNodeResult.succeeded("ok"),
        }
    )

    assert evaluation.status.value == "UNMATCHED"


def test_cycle_search_skips_nonremaining_targets_and_backtracks():
    builder = _FlowBuilder()
    a = builder.add_node(return_name, "A")
    branch = builder.add_node(return_name, "branch")
    b = builder.add_node(return_name, "B")
    outside = builder.add_node(return_name, "outside")
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
        a = node(return_name, name="A")
        b = node(return_name, name="B")
        a >> b
        captured.update(a=a, b=b)
        return b

    _evaluate_definition(graph())

    async def fail():
        msg = "unexpected dependency failure"
        raise ValueError(msg)

    task = asyncio.create_task(fail())
    with pytest.raises(ValueError, match="unexpected dependency"):
        await _resolve_dependencies(
            captured["b"]._dependency,
            {captured["a"]: task},
        )


def test_flow_node_context_reuses_existing_step_counter():
    state = Mock(spec=ExecutionState)
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
        return node(return_name, name="A")

    frozen = _evaluate_definition(graph())

    async def fail():
        msg = "unexpected child failure"
        raise ValueError(msg)

    def fake_child(*args, **kwargs):
        return asyncio.create_task(fail())

    monkeypatch.setattr(
        "async_durable_execution.composite.flow.run_in_child_context",
        fake_child,
    )

    with pytest.raises(_FlowControlSignal) as raised:
        await _execute_flow(frozen)
    assert isinstance(raised.value.error, ValueError)


@pytest.mark.parametrize(
    ("child_error", "expected_error"),
    [
        (
            CallableRuntimeError(
                message="serialized control",
                error_type="ExecutionError",
                data=None,
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
        "async_durable_execution.composite.flow.run_in_child_context",
        fake_child,
    )

    with bind_current_context(context):
        flow_task = flow(graph())
        with pytest.raises(expected_error):
            await flow_task

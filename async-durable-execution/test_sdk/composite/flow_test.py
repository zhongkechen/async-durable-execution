"""Tests for declarative acyclic durable flows."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast
from unittest.mock import Mock

import pytest

from async_durable_execution import (
    BatchResult,
    DurableContext,
    ErrorObject,
    FlowDefinitionError,
    FlowExecutionError,
    FlowNodeContext,
    FlowNodeResult,
    FlowNodeStatus,
    FlowResult,
    InvalidStateError,
    InvocationStatus,
    RetryStrategy,
    create_local_runner,
    durable_callable,
    durable_dag,
    durable_execution,
    durable_node,
    flow,
    get_current_context,
    node,
    parallel,
    step,
    wait,
)
from async_durable_execution.context import bind_current_context
from async_durable_execution.composite.flow import _evaluate_definition
from async_durable_execution.models import (
    OperationIdentifier,
    OperationStatus,
    OperationSubType,
)
from async_durable_execution.state import ExecutionState


@durable_node
async def return_name() -> str:
    return cast(FlowNodeContext, get_current_context()).operation_name or ""


def create_test_context() -> tuple[DurableContext, Mock]:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = (
        "arn:aws:durable:us-east-1:123456789012:execution/test"
    )
    state.operations.get.return_value = None
    context = DurableContext(
        execution_state=state,
        operation_identifier=OperationIdentifier(
            operation_id=None,
            sub_type=OperationSubType.EXECUTION,
        ),
    )
    return context, state


def test_durable_dag_binds_arguments_without_running_definition():
    calls: list[str] = []

    @durable_dag
    def graph(value: str):
        calls.append(value)
        return None

    bound = graph("bound")

    assert calls == []
    assert bound.__name__ == "graph"


def test_durable_dag_rejects_async_definition():
    with pytest.raises(FlowDefinitionError, match="synchronous"):

        @durable_dag
        async def invalid_graph():
            return None


async def test_durable_node_binds_arguments_without_running_function():
    calls: list[str] = []

    @durable_node
    async def fetch(order_id: str, *, region: str) -> str:
        calls.append(order_id)
        return f"{region}:{order_id}"

    bound = fetch("order-123", region="us-west-2")

    assert calls == []
    assert bound.__name__ == "fetch"
    assert getattr(bound, "_durable_node_callable")
    assert await bound() == "us-west-2:order-123"
    assert calls == ["order-123"]


async def test_durable_node_rejects_invalid_arguments_before_checkpoint():
    context, state = create_test_context()

    @durable_node
    async def required(value: str) -> str:
        return value

    invalid_calls = (
        lambda: required(),
        lambda: required("first", "second"),
        lambda: required("value", unknown=True),
    )
    with bind_current_context(context):
        for invalid_call in invalid_calls:

            @durable_dag
            def graph():
                return node(invalid_call()).outcome

            with pytest.raises(TypeError):
                flow(graph())

    state.create_checkpoint.assert_not_called()


def test_durable_node_rejects_synchronous_function():
    with pytest.raises(FlowDefinitionError, match="async"):

        @durable_node
        def invalid_node():
            return None


def test_node_outside_definition_is_rejected():
    with pytest.raises(InvalidStateError, match=r"node\(\)"):
        node(return_name(), name="outside")


async def test_plain_callable_is_not_a_flow_definition():
    context, state = create_test_context()

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="@durable_dag"):
            flow(lambda: None)

    state.create_checkpoint.assert_not_called()


async def test_node_requires_bound_durable_node_callable_before_checkpoint():
    context, state = create_test_context()

    @durable_dag
    def invalid_graph():
        node(return_name, name="invalid")

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="@durable_node"):
            flow(invalid_graph())

    state.create_checkpoint.assert_not_called()


def test_node_uses_durable_node_function_name_by_default():
    captured = {}

    @durable_dag
    def graph():
        captured["node"] = node(return_name())

    _evaluate_definition(graph())

    assert captured["node"].name == "return_name"


async def test_duplicate_default_node_names_fail_before_checkpoint():
    context, state = create_test_context()

    @durable_dag
    def invalid_graph():
        node(return_name())
        node(return_name())

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="duplicated"):
            flow(invalid_graph())

    state.create_checkpoint.assert_not_called()


async def test_unawaited_durable_operation_is_rejected_during_definition():
    context, state = create_test_context()

    @durable_dag
    def invalid_graph():
        step(return_name, name="not-allowed")
        return None

    with bind_current_context(context):
        with pytest.raises(InvalidStateError, match="defining a flow"):
            flow(invalid_graph())

    state.create_checkpoint.assert_not_called()


@pytest.mark.parametrize("name", ["", "   "])
async def test_empty_node_name_fails_before_checkpoint(name: str):
    context, state = create_test_context()

    @durable_dag
    def invalid_graph():
        node(return_name(), name=name)

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="non-empty"):
            flow(invalid_graph())

    state.create_checkpoint.assert_not_called()


async def test_duplicate_node_name_fails_before_checkpoint():
    context, state = create_test_context()

    @durable_dag
    def invalid_graph():
        node(return_name(), name="same")
        node(return_name(), name="same")

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="duplicated"):
            flow(invalid_graph())

    state.create_checkpoint.assert_not_called()


async def test_repeated_target_expression_requires_explicit_operator():
    context, state = create_test_context()

    @durable_dag
    def invalid_graph():
        a = node(return_name(), name="A")
        b = node(return_name(), name="B")
        c = node(return_name(), name="C")
        a >> c
        b >> c

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="already has"):
            flow(invalid_graph())

    state.create_checkpoint.assert_not_called()


async def test_duplicate_dependency_and_self_edge_are_rejected():
    context, state = create_test_context()

    @durable_dag
    def duplicate_graph():
        a = node(return_name(), name="A")
        b = node(return_name(), name="B")
        (a & a) >> b

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="duplicate dependency"):
            flow(duplicate_graph())

    @durable_dag
    def self_graph():
        a = node(return_name(), name="A")
        a >> a

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="depend on itself"):
            flow(self_graph())

    state.create_checkpoint.assert_not_called()


async def test_cycle_error_contains_concrete_stable_path():
    context, state = create_test_context()

    @durable_dag
    def cyclic_graph():
        a = node(return_name(), name="A")
        b = node(return_name(), name="B")
        c = node(return_name(), name="C")
        a >> b
        b >> c
        c >> a

    with bind_current_context(context):
        with pytest.raises(
            FlowDefinitionError,
            match=r"A -> B -> C -> A",
        ):
            flow(cyclic_graph())

    state.create_checkpoint.assert_not_called()


async def test_invalid_definition_output_fails_before_checkpoint():
    context, state = create_test_context()

    @durable_dag
    def raw_value_graph():
        return "A"

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="must return"):
            flow(raw_value_graph())

    @durable_dag
    def raw_node_graph():
        return node(return_name(), name="A")

    with bind_current_context(context):
        with pytest.raises(
            FlowDefinitionError,
            match=r"cannot return a FlowNode directly.*node\.outcome",
        ):
            flow(raw_node_graph())

    state.create_checkpoint.assert_not_called()


async def test_linear_fanout_fanin_flow_checkpoints_complete_result():
    @durable_dag
    def graph(value: str):
        @durable_node
        async def run_a(node_value: str) -> str:
            assert isinstance(get_current_context(), FlowNodeContext)
            return node_value

        @durable_node
        async def run_b() -> str:
            return f"{cast(FlowNodeContext, get_current_context()).result(a).outcome}-B"

        @durable_node
        async def run_c() -> str:
            return f"{cast(FlowNodeContext, get_current_context()).result(a).outcome}-C"

        @durable_node
        async def run_d() -> str:
            return f"{cast(FlowNodeContext, get_current_context()).result(b).outcome}+{cast(FlowNodeContext, get_current_context()).result(c).outcome}"

        a = node(run_a(value), name="A")
        b = node(run_b(), name="B")
        c = node(run_c(), name="C")
        d = node(run_d(), name="D")
        a >> (b, c)
        (b & c) >> d
        return d.outcome

    @durable_execution
    async def handler(event):
        result = await flow(graph(event["value"]), name="diamond")
        return result.to_dict()

    async with create_local_runner(
        handler=handler,
        input={"value": "root"},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert [payload["results"][name]["status"] for name in ("A", "B", "C", "D")] == [
        "SUCCEEDED",
        "SUCCEEDED",
        "SUCCEEDED",
        "SUCCEEDED",
    ]
    assert payload["outputs"] == ["root-B+root-C"]
    assert payload["outputProjections"] == ["OUTCOME"]
    assert payload["unhandledFailures"] == []

    flow_operation = result.get_context("diamond")
    assert flow_operation.status is OperationStatus.SUCCEEDED
    node_operations = result.get_child_operations(flow_operation)
    assert [operation.name for operation in node_operations] == ["A", "B", "C", "D"]
    assert all(
        operation.status is OperationStatus.SUCCEEDED for operation in node_operations
    )


async def test_node_outcome_argument_infers_success_dependency_and_resolves_value():
    @durable_dag
    def graph():
        @durable_node
        async def source() -> dict[str, str]:
            return {"payment": "accepted"}

        @durable_node
        async def consume(payload: dict[str, object]) -> str:
            values = cast("list[object]", payload["values"])
            nested = cast("tuple[object]", values[1])
            assert values[0] is nested[0]
            payment = cast("dict[str, str]", values[0])
            return payment["payment"]

        source_node = node(source())
        return node(
            consume(
                {
                    "values": [
                        source_node.outcome,
                        (source_node.outcome,),
                    ]
                }
            )
        ).outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="inferred-success")).to_dict()

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["results"]["source"]["status"] == "SUCCEEDED"
    assert payload["results"]["consume"]["outcome"] == "accepted"


async def test_batch_result_outcome_preserves_type_across_flow_checkpoints():
    @durable_callable
    async def branch(value: str) -> str:
        return value

    @durable_dag
    def graph():
        @durable_node
        async def source() -> BatchResult[str]:
            return await parallel(
                [branch("first"), branch("second")],
                name="parallel-work",
            )

        @durable_node
        async def consume(batch_result: BatchResult[str]) -> list[str]:
            assert isinstance(batch_result, BatchResult)
            return batch_result.get_results()

        source_node = node(source(), name="source")
        return node(
            consume(source_node.outcome),
            name="consume",
        ).outcome

    @durable_execution
    async def handler(event):
        result = await flow(graph(), name="batch-result-flow")
        source_outcome = result.get_result("source").outcome
        assert isinstance(source_outcome, BatchResult)
        return result.output

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert json.loads(result.result) == ["first", "second"]


async def test_node_error_argument_infers_failure_dependency_and_resolves_error():
    @durable_dag
    def graph():
        @durable_node
        async def fail() -> None:
            msg = "payment declined"
            raise ValueError(msg)

        @durable_node
        async def recover(error: ErrorObject | None) -> str:
            assert error is not None
            return error.message or ""

        source = node(fail())
        return node(recover(source.error)).outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="inferred-failure")).to_dict()

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["results"]["fail"]["status"] == "FAILED"
    assert payload["results"]["recover"]["outcome"] == "payment declined"
    assert payload["unhandledFailures"] == []


async def test_dag_outputs_project_outcome_error_and_result():
    captured_output = None

    @durable_dag
    def graph():
        @durable_node
        async def succeed() -> str:
            return "value"

        @durable_node
        async def fail() -> None:
            msg = "expected failure"
            raise ValueError(msg)

        success = node(succeed(), name="success")
        failure = node(fail(), name="failure")
        return success.outcome, failure.error, failure.result()

    @durable_execution
    async def handler(event):
        nonlocal captured_output
        result = await flow(graph(), name="projected-outputs")
        captured_output = result.output
        return result.to_dict()

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert isinstance(captured_output, tuple)
    assert captured_output[0] == "value"
    assert isinstance(captured_output[1], ErrorObject)
    assert captured_output[1].message == "expected failure"
    assert isinstance(captured_output[2], FlowNodeResult)
    assert captured_output[2].status is FlowNodeStatus.FAILED

    payload = json.loads(result.result)
    assert payload["outputs"][0] == "value"
    assert payload["outputs"][1]["ErrorMessage"] == "expected failure"
    assert payload["outputs"][2]["status"] == "FAILED"
    assert payload["outputProjections"] == ["OUTCOME", "ERROR", "RESULT"]
    assert payload["unhandledFailures"] == []


async def test_required_inputs_and_explicit_dependency_are_combined_with_all():
    @durable_dag
    def graph():
        @durable_node
        async def source() -> str:
            return "source"

        @durable_node
        async def gate() -> str:
            return "open"

        @durable_node
        async def consume(value: str) -> str:
            context = cast(FlowNodeContext, get_current_context())
            assert context.require_dependency_result("gate").outcome == "open"
            return value

        source_node = node(source())
        gate_node = node(gate())
        return node(
            consume(source_node.outcome),
            dependency=gate_node.succeeded,
        ).outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="combined-dependencies")).to_dict()

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["results"]["consume"]["outcome"] == "source"


async def test_dependency_argument_exposes_stable_result_snapshot_by_name():
    release_pending = asyncio.Event()

    @durable_dag
    def graph():
        @durable_node
        async def pending_branch() -> str:
            await release_pending.wait()
            return "later"

        @durable_node
        async def winner() -> str:
            return "winner"

        @durable_node
        async def handle() -> str:
            context = cast(FlowNodeContext, get_current_context())
            assert context.get_dependency_result("pending") is None
            with pytest.raises(InvalidStateError, match="not available"):
                context.require_dependency_result("pending")
            with pytest.raises(InvalidStateError, match="not a direct dependency"):
                context.get_dependency_result("missing")

            result = context.require_dependency_result("winner")
            assert context.dependency_results == {"winner": result}
            release_pending.set()
            return cast(str, result.outcome)

        pending = node(pending_branch(), name="pending")
        completed = node(winner(), name="winner")
        return node(
            handle(),
            name="handler",
            dependency=pending.failed | completed.succeeded,
        ).outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="dependency-snapshot")).to_dict()

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["results"]["handler"]["outcome"] == "winner"
    assert payload["results"]["pending"]["outcome"] == "later"


def test_node_inputs_reject_conflicting_and_duplicate_explicit_dependencies():
    @durable_node
    async def consume(first: object, second: object) -> None:
        _ = first, second

    @durable_dag
    def conflicting_graph():
        source = node(return_name(), name="source")
        node(
            consume(source.outcome, source.error),
            name="target",
        )

    with pytest.raises(FlowDefinitionError, match="multiple projections"):
        _evaluate_definition(conflicting_graph())

    @durable_dag
    def duplicate_graph():
        source = node(return_name(), name="source")
        node(
            consume(source.outcome, "value"),
            name="target",
            dependency=source.succeeded,
        )

    with pytest.raises(FlowDefinitionError, match="both a required input"):
        _evaluate_definition(duplicate_graph())


@pytest.mark.parametrize(
    ("container_kind", "container_pattern"),
    [
        pytest.param("set", r"'set'", id="set"),
        pytest.param("frozenset", r"'frozenset'", id="frozenset"),
        pytest.param("dataclass", r"'.*ProjectionPayload'", id="dataclass"),
    ],
)
def test_node_inputs_reject_projection_nested_in_unsupported_container(
    container_kind: str,
    container_pattern: str,
):
    @dataclass(frozen=True)
    class ProjectionPayload:
        value: object

    @durable_node
    async def consume(value: object) -> None:
        _ = value

    @durable_dag
    def graph():
        source = node(return_name(), name="source")
        projection = source.outcome
        if container_kind == "set":
            nested_input: object = {projection}
        elif container_kind == "frozenset":
            nested_input = frozenset({projection})
        else:
            nested_input = ProjectionPayload(projection)
        target = node(consume(nested_input), name="target")
        return target.outcome

    with pytest.raises(
        FlowDefinitionError,
        match=rf"unsupported container type {container_pattern}",
    ):
        _evaluate_definition(graph())


async def test_flow_node_handle_exposes_result_status_outcome_and_error():
    @durable_dag
    def graph():
        @durable_node
        async def source() -> str:
            return "source"

        @durable_node
        async def target() -> str:
            source_result = source_node.result()
            assert source_result.status is FlowNodeStatus.SUCCEEDED
            assert source_node.status is FlowNodeStatus.SUCCEEDED
            assert source_node.error is None
            return f"{source_node.outcome}-target"

        source_node = node(source(), name="source")
        target_node = node(target(), name="target")
        source_node >> target_node
        return target_node.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="node-result-access")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert payload["outputs"] == ["source-target"]


async def test_failure_route_skips_success_branch_and_handles_source_failure():
    called: list[str] = []

    @durable_dag
    def graph():
        @durable_node
        async def run_a() -> str:
            called.append("A")
            msg = "A failed"
            raise ValueError(msg)

        @durable_node
        async def run_b() -> str:
            called.append("B")
            return "unexpected"

        @durable_node
        async def run_c() -> str:
            called.append("C")
            result = cast(FlowNodeContext, get_current_context()).result(a)
            assert result.status is FlowNodeStatus.FAILED
            assert result.error is not None
            return result.error.message or ""

        @durable_node
        async def run_d() -> str:
            called.append("D")
            assert (
                cast(FlowNodeContext, get_current_context()).result(c).status
                is FlowNodeStatus.SUCCEEDED
            )
            return "recovered"

        a = node(run_a(), name="A")
        b = node(run_b(), name="B")
        c = node(run_c(), name="C")
        d = node(run_d(), name="D")
        a >> b
        a.failed >> c
        (b.succeeded | c.succeeded) >> d
        return d.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="failure-route")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert called == ["A", "C", "D"]
    assert payload["results"]["A"]["status"] == "FAILED"
    assert payload["results"]["B"]["status"] == "SKIPPED"
    assert payload["results"]["C"]["outcome"] == "A failed"
    assert payload["results"]["D"]["outcome"] == "recovered"
    assert payload["unhandledFailures"] == []


async def test_flow_node_failure_properties_remain_available():
    @durable_dag
    def graph():
        @durable_node
        async def fail() -> None:
            msg = "source failed"
            raise ValueError(msg)

        @durable_node
        async def recover() -> str:
            source_result = source.result()
            assert source_result.status is FlowNodeStatus.FAILED
            assert source_result.error is not None
            assert source.status is FlowNodeStatus.FAILED
            assert source.error == source_result.error
            with pytest.raises(
                InvalidStateError,
                match=r"did not succeed \(status FAILED\)",
            ):
                _ = source.outcome
            return source_result.error.message or ""

        source = node(fail(), name="source")
        recovery = node(recover(), name="recovery")
        source.failed >> recovery
        return recovery.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="failed-node-result-access")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert payload["outputs"] == ["source failed"]
    assert payload["unhandledFailures"] == []


async def test_unhandled_failure_raises_after_flow_result_is_checkpointed():
    captured_result = None

    @durable_dag
    def graph():
        @durable_node
        async def fail() -> None:
            msg = "unhandled"
            raise RuntimeError(msg)

        return node(fail(), name="failure").outcome

    @durable_execution
    async def handler(event):
        nonlocal captured_result
        try:
            await flow(graph(), name="unhandled-flow")
        except FlowExecutionError as error:
            captured_result = error.result
            return error.result.to_dict()
        pytest.fail("FlowExecutionError was not raised")

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert captured_result is not None
    assert captured_result.unhandled_failures == ("failure",)
    payload = json.loads(result.result)
    assert payload["results"]["failure"]["status"] == "FAILED"
    assert payload["unhandledFailures"] == ["failure"]
    assert result.get_context("unhandled-flow").status is OperationStatus.SUCCEEDED


async def test_handled_failure_outcome_is_reported_as_unavailable():
    @durable_dag
    def graph():
        @durable_node
        async def fail() -> None:
            msg = "expected failure"
            raise ValueError(msg)

        @durable_node
        async def recover(error: ErrorObject | None) -> str:
            assert error is not None
            return "recovered"

        source = node(fail(), name="source")
        recovery = node(recover(source.error), name="recovery")
        return source.outcome, recovery.outcome

    @durable_execution
    async def handler(event):
        try:
            await flow(graph(), name="handled-unavailable-output")
        except FlowExecutionError as error:
            return error.result.to_dict()
        pytest.fail("The unavailable source outcome must fail the flow")

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert payload["outputs"] == [None, "recovered"]
    assert payload["unhandledFailures"] == []
    assert payload["unavailableOutputs"] == ["source"]
    assert (
        result.get_context("handled-unavailable-output").status
        is OperationStatus.SUCCEEDED
    )


async def test_skipped_outcome_is_reported_as_unavailable():
    called: list[str] = []

    @durable_dag
    def graph():
        @durable_node
        async def fail() -> None:
            msg = "expected failure"
            raise ValueError(msg)

        @durable_node
        async def only_on_success() -> str:
            called.append("target")
            return "unreachable"

        @durable_node
        async def recover(error: ErrorObject | None) -> str:
            assert error is not None
            return "recovered"

        source = node(fail(), name="source")
        target = node(only_on_success(), name="target")
        recovery = node(recover(source.error), name="recovery")
        source.succeeded >> target
        return target.outcome, recovery.outcome

    @durable_execution
    async def handler(event):
        try:
            await flow(graph(), name="skipped-unavailable-output")
        except FlowExecutionError as error:
            return error.result.to_dict()
        pytest.fail("The skipped target outcome must fail the flow")

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert called == []
    assert payload["results"]["target"]["status"] == "SKIPPED"
    assert payload["unhandledFailures"] == []
    assert payload["unavailableOutputs"] == ["target"]


async def test_node_can_run_durable_operations_in_isolated_scope(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0")
    step_calls: list[str] = []
    node_calls: list[str] = []

    @durable_callable
    async def checkpoint(value: str) -> str:
        step_calls.append(value)
        return value

    @durable_dag
    def graph():
        @durable_node
        async def run_a() -> str:
            node_calls.append("A")
            assert isinstance(get_current_context(), FlowNodeContext)
            return await step(checkpoint("A"), name="inside-A")

        @durable_node
        async def run_b() -> str:
            node_calls.append("B")
            assert cast(FlowNodeContext, get_current_context()).result(a).outcome == "A"
            await wait(timedelta(seconds=1), name="inside-B-wait")
            return await step(checkpoint("B"), name="inside-B")

        a = node(run_a(), name="A")
        b = node(run_b(), name="B")
        a >> b
        return a.outcome, b.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="nested-operations")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert payload["outputs"] == ["A", "B"]
    assert step_calls == ["A", "B"]
    assert node_calls == ["A", "B", "B"]

    flow_operation = result.get_context("nested-operations")
    node_operations = {
        operation.name: operation
        for operation in result.get_child_operations(flow_operation)
    }
    step_a = next(
        operation
        for operation in result.get_child_operations(node_operations["A"])
        if operation.name == "inside-A"
    )
    step_b = next(
        operation
        for operation in result.get_child_operations(node_operations["B"])
        if operation.name == "inside-B"
    )
    assert step_a.operation_id != step_b.operation_id
    assert step_a.parent_id == node_operations["A"].operation_id
    assert step_b.parent_id == node_operations["B"].operation_id


async def test_any_starts_on_first_matching_result_and_does_not_handle_later_failure():
    release_failure = asyncio.Event()
    called: list[str] = []

    @durable_dag
    def graph():
        @durable_node
        async def fail_later() -> None:
            called.append("A")
            await release_failure.wait()
            await asyncio.sleep(0.01)
            msg = "late failure"
            raise ValueError(msg)

        @durable_node
        async def succeed_first() -> str:
            called.append("B")
            return "winner"

        @durable_node
        async def handle() -> str:
            called.append("handler")
            assert (
                cast(FlowNodeContext, get_current_context()).result(b).outcome
                == "winner"
            )
            with pytest.raises(InvalidStateError, match="not available"):
                cast(FlowNodeContext, get_current_context()).result(a)
            release_failure.set()
            return "handled winner"

        a = node(fail_later(), name="A")
        b = node(succeed_first(), name="B")
        handler = node(handle(), name="handler")
        (a.failed | b.succeeded) >> handler
        return handler.outcome

    @durable_execution
    async def handler(event):
        try:
            await flow(graph(), name="first-match")
        except FlowExecutionError as error:
            return error.result.to_dict()
        pytest.fail("The later failure must remain unhandled")

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert called == ["A", "B", "handler"]
    assert payload["results"]["handler"]["outcome"] == "handled winner"
    assert payload["results"]["A"]["status"] == "FAILED"
    assert payload["unhandledFailures"] == ["A"]


async def test_any_ignores_nonmatching_terminal_result():
    release_second = asyncio.Event()

    @durable_dag
    def graph():
        @durable_node
        async def fail_unmatched() -> None:
            release_second.set()
            msg = "not a success"
            raise ValueError(msg)

        @durable_node
        async def fail_matched() -> None:
            await release_second.wait()
            await asyncio.sleep(0.01)
            msg = "matched failure"
            raise RuntimeError(msg)

        @durable_node
        async def handle() -> str:
            assert (
                cast(FlowNodeContext, get_current_context()).result(a).status
                is FlowNodeStatus.FAILED
            )
            assert (
                cast(FlowNodeContext, get_current_context()).result(b).status
                is FlowNodeStatus.FAILED
            )
            return "B handled"

        a = node(fail_unmatched(), name="A")
        b = node(fail_matched(), name="B")
        handler = node(handle(), name="handler")
        (a.succeeded | b.failed) >> handler
        return handler.outcome

    @durable_execution
    async def handler(event):
        try:
            await flow(graph(), name="skip-nonmatch")
        except FlowExecutionError as error:
            return error.result.to_dict()
        pytest.fail("A must remain unhandled")

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert payload["results"]["handler"]["outcome"] == "B handled"
    assert payload["unhandledFailures"] == ["A"]


async def test_any_winner_survives_partial_replay(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0")
    release_definition_first = asyncio.Event()
    definition_first_finished = asyncio.Event()
    observed_winners: list[str] = []

    @durable_dag
    def graph():
        @durable_node
        async def definition_first() -> str:
            await release_definition_first.wait()
            definition_first_finished.set()
            return "definition-first"

        @durable_node
        async def completes_first() -> None:
            msg = "completion-first failure"
            raise ValueError(msg)

        @durable_node
        async def handle() -> str:
            context = cast(FlowNodeContext, get_current_context())
            assert context.result(a).status is FlowNodeStatus.FAILED
            with pytest.raises(InvalidStateError, match="not available"):
                context.result(b)
            observed_winners.append("A")

            release_definition_first.set()
            await definition_first_finished.wait()
            await asyncio.sleep(0.01)
            await wait(timedelta(seconds=1), name="handler-wait")
            return "handled"

        # B is first in definition/expression order, but A completes first.
        b = node(definition_first(), name="B")
        a = node(completes_first(), name="A")
        handler = node(handle(), name="handler")
        (b.succeeded | a.failed) >> handler
        return handler.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="replayed-any-winner")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["results"]["handler"]["outcome"] == "handled"
    assert payload["unhandledFailures"] == []
    assert observed_winners == ["A", "A"]


async def test_any_matching_sibling_runs_before_suspended_branch_resumes(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0")
    observed: list[str] = []

    @durable_dag
    def graph():
        @durable_node
        async def waiting() -> str:
            await wait(timedelta(seconds=1), name="waiting-delay")
            observed.append("waiting-completed")
            return "waiting"

        @durable_node
        async def ready() -> str:
            return "ready"

        @durable_node
        async def handle() -> str:
            context = cast(FlowNodeContext, get_current_context())
            assert context.get_dependency_result("waiting") is None
            assert context.require_dependency_result("ready").outcome == "ready"
            observed.append("target")
            return "handled"

        waiting_node = node(waiting(), name="waiting")
        ready_node = node(ready(), name="ready")
        target = node(handle(), name="target")
        (waiting_node.succeeded | ready_node.succeeded) >> target
        return target.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="suspended-any")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert payload["results"]["target"]["outcome"] == "handled"
    assert observed == ["target", "waiting-completed"]


async def test_nested_any_winner_survives_outer_all_partial_replay(monkeypatch):
    from async_durable_execution.composite.flow import (
        _NodeExecutionSerDes,
        _PersistedDependencyResolutionSerDes,
    )

    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0")
    winner_persisted = asyncio.Event()
    release_b = asyncio.Event()
    b_persisted = asyncio.Event()
    c_calls = 0
    observed_dependencies: list[tuple[str, str]] = []

    dependency_deserialize = _PersistedDependencyResolutionSerDes.deserialize

    async def observe_dependency_checkpoint(self, data):
        resolution = await dependency_deserialize(self, data)
        if resolution.selected_nodes == ("A",):
            winner_persisted.set()
        return resolution

    node_deserialize = _NodeExecutionSerDes.deserialize

    async def observe_node_checkpoint(self, data):
        execution = await node_deserialize(self, data)
        if execution.result.outcome == "B":
            b_persisted.set()
        return execution

    monkeypatch.setattr(
        _PersistedDependencyResolutionSerDes,
        "deserialize",
        observe_dependency_checkpoint,
    )
    monkeypatch.setattr(
        _NodeExecutionSerDes,
        "deserialize",
        observe_node_checkpoint,
    )

    @durable_dag
    def graph():
        @durable_node
        async def definition_first() -> str:
            await release_b.wait()
            return "B"

        @durable_node
        async def completes_first() -> None:
            msg = "A failed first"
            raise ValueError(msg)

        @durable_node
        async def suspends_outer_all() -> str:
            nonlocal c_calls
            c_calls += 1
            await winner_persisted.wait()
            release_b.set()
            await b_persisted.wait()
            await wait(timedelta(seconds=1), name="C-wait")
            return "C"

        @durable_node
        async def target() -> str:
            context = cast(FlowNodeContext, get_current_context())
            a_result = context.result(a)
            c_result = context.result(c)
            assert a_result.status is FlowNodeStatus.FAILED
            assert c_result.outcome == "C"
            with pytest.raises(InvalidStateError, match="not available"):
                context.result(b)
            observed_dependencies.append((a_result.status.value, c_result.outcome))
            return "target"

        # B is first in expression order, but A wins before B is released.
        b = node(definition_first(), name="B")
        a = node(completes_first(), name="A")
        c = node(suspends_outer_all(), name="C")
        target_node = node(target(), name="target")
        ((b.succeeded | a.failed) & c.succeeded) >> target_node
        return target_node.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="replayed-nested-any-winner")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["results"]["B"]["status"] == "SUCCEEDED"
    assert payload["results"]["target"]["outcome"] == "target"
    assert payload["unhandledFailures"] == []
    assert c_calls == 2
    assert observed_dependencies == [("FAILED", "C")]


def test_nested_any_does_not_retroactively_change_its_winner():
    handles = {}

    @durable_dag
    def graph():
        a = node(return_name(), name="A")
        b = node(return_name(), name="B")
        c = node(return_name(), name="C")
        d = node(return_name(), name="D")
        ((a.failed | b.succeeded) & c.succeeded) >> d
        handles.update(a=a, b=b, c=c, d=d)
        return d.outcome

    from async_durable_execution.composite.flow import _evaluate_definition

    _evaluate_definition(graph())
    expression = handles["d"]._dependency
    assert expression is not None
    winners: dict[int, int] = {}

    first_evaluation = expression.evaluate(
        {
            handles["b"]: FlowNodeResult.succeeded("winner"),
        },
        winners,
    )
    assert first_evaluation.status.value == "PENDING"

    final_evaluation = expression.evaluate(
        {
            handles["a"]: FlowNodeResult.failed(ErrorObject.from_message("late")),
            handles["b"]: FlowNodeResult.succeeded("winner"),
            handles["c"]: FlowNodeResult.succeeded("all-ready"),
        },
        winners,
    )
    assert final_evaluation.status.value == "MATCHED"
    assert final_evaluation.handled_failures == ()


async def test_all_waits_for_every_dependency_before_running():
    second_finished = False

    @durable_dag
    def graph():
        @durable_node
        async def first() -> str:
            return "first"

        @durable_node
        async def second() -> str:
            nonlocal second_finished
            await asyncio.sleep(0.01)
            second_finished = True
            return "second"

        @durable_node
        async def combined() -> str:
            assert second_finished
            return f"{cast(FlowNodeContext, get_current_context()).result(a).outcome}+{cast(FlowNodeContext, get_current_context()).result(b).outcome}"

        a = node(first(), name="A")
        b = node(second(), name="B")
        c = node(combined(), name="C")
        (a & b) >> c
        return c.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="all-dependencies")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert payload["outputs"] == ["first+second"]


async def test_completed_route_does_not_handle_failure():
    @durable_dag
    def graph():
        @durable_node
        async def fail() -> None:
            msg = "failure"
            raise ValueError(msg)

        @durable_node
        async def observe() -> str:
            assert (
                cast(FlowNodeContext, get_current_context()).result(a).status
                is FlowNodeStatus.FAILED
            )
            return "observed"

        a = node(fail(), name="A")
        observer = node(observe(), name="observer")
        a.completed >> observer
        return observer.outcome

    @durable_execution
    async def handler(event):
        try:
            await flow(graph(), name="completed-route")
        except FlowExecutionError as error:
            return error.result.to_dict()
        pytest.fail(".completed must not handle a failure")

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["results"]["observer"]["outcome"] == "observed"
    assert payload["unhandledFailures"] == ["A"]


async def test_handler_failure_is_evaluated_independently():
    @durable_dag
    def graph():
        @durable_node
        async def source() -> None:
            msg = "source"
            raise ValueError(msg)

        @durable_node
        async def handler() -> None:
            msg = "handler"
            raise RuntimeError(msg)

        a = node(source(), name="source")
        b = node(handler(), name="handler")
        a.failed >> b
        return b.outcome

    @durable_execution
    async def handler(event):
        try:
            await flow(graph(), name="handler-failure")
        except FlowExecutionError as error:
            return error.result.to_dict()
        pytest.fail("The handler failure must be unhandled")

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["results"]["source"]["status"] == "FAILED"
    assert payload["results"]["handler"]["status"] == "FAILED"
    assert payload["unhandledFailures"] == ["handler"]


async def test_invalid_dependency_result_access_is_a_logical_node_failure():
    @durable_dag
    def graph():
        @durable_node
        async def root() -> str:
            return cast(FlowNodeContext, get_current_context()).operation_name or ""

        @durable_node
        async def invalid() -> str:
            cast(FlowNodeContext, get_current_context()).result(b)
            return "unreachable"

        @durable_node
        async def recover() -> str:
            result = cast(FlowNodeContext, get_current_context()).result(c)
            assert result.error is not None
            return result.error.type or ""

        a = node(root(), name="A")
        b = node(root(), name="B")
        c = node(invalid(), name="C")
        recovery = node(recover(), name="recovery")
        a >> c
        c.failed >> recovery
        return recovery.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="invalid-result-access")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["results"]["C"]["status"] == "FAILED"
    assert payload["results"]["recovery"]["outcome"] == "InvalidStateError"
    assert payload["unhandledFailures"] == []


async def test_failure_handling_metadata_survives_partial_replay(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0")
    source_calls = 0
    handler_calls = 0

    @durable_dag
    def graph():
        @durable_node
        async def source() -> None:
            nonlocal source_calls
            source_calls += 1
            msg = "source failed"
            raise ValueError(msg)

        @durable_node
        async def recover() -> str:
            nonlocal handler_calls
            handler_calls += 1
            assert (
                cast(FlowNodeContext, get_current_context()).result(a).status
                is FlowNodeStatus.FAILED
            )
            await wait(timedelta(seconds=1), name="recovery-wait")
            return "recovered"

        a = node(source(), name="source")
        recovery = node(recover(), name="recovery")
        a.failed >> recovery
        return recovery.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="replayed-recovery")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["unhandledFailures"] == []
    assert payload["outputs"] == ["recovered"]
    assert source_calls == 1
    assert handler_calls == 2


async def test_sdk_control_error_does_not_activate_failure_route():
    from async_durable_execution import ExecutionError

    called: list[str] = []

    @durable_dag
    def graph():
        @durable_node
        async def control_failure() -> None:
            called.append("source")
            msg = "SDK control failure"
            raise ExecutionError(msg)

        @durable_node
        async def should_not_run() -> None:
            called.append("handler")

        a = node(control_failure(), name="source")
        handler = node(should_not_run(), name="handler")
        a.failed >> handler
        return handler.outcome

    @durable_execution
    async def handler(event):
        await flow(graph(), name="control-error")

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert result.error.type == "ExecutionError"
    assert called == ["source"]


async def test_user_execution_error_from_step_activates_failure_route():
    class ExecutionError(Exception):
        pass

    called: list[str] = []

    @durable_callable
    async def fail_step() -> None:
        msg = "User failure"
        raise ExecutionError(msg)

    @durable_dag
    def graph():
        @durable_node
        async def source() -> None:
            called.append("source")
            await step(
                fail_step(),
                name="user-failure-step",
                retry_strategy=RetryStrategy.none(),
            )

        @durable_node
        async def recover() -> str:
            called.append("recovery")
            result = cast(FlowNodeContext, get_current_context()).result(a)
            assert result.error is not None
            assert result.error.type == "ExecutionError"
            return "recovered"

        a = node(source(), name="source")
        recovery = node(recover(), name="recovery")
        a.failed >> recovery
        return recovery.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="user-execution-error")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    payload = json.loads(result.result)
    assert payload["outputs"] == ["recovered"]
    assert payload["unhandledFailures"] == []
    assert called == ["source", "recovery"]


async def test_empty_and_disconnected_flows():
    @durable_dag
    def empty_graph():
        return None

    @durable_dag
    def disconnected_graph():
        node(return_name(), name="A")
        node(return_name(), name="B")
        return None

    @durable_execution
    async def handler(event):
        empty_result = await flow(empty_graph(), name="empty")
        disconnected_result = await flow(disconnected_graph(), name="disconnected")
        return {
            "empty": empty_result.to_dict(),
            "disconnected": disconnected_result.to_dict(),
        }

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["empty"] == {
        "results": {},
        "outputs": [],
        "outputProjections": [],
        "unhandledFailures": [],
        "unavailableOutputs": [],
    }
    assert list(payload["disconnected"]["results"]) == ["A", "B"]
    assert {
        result["status"] for result in payload["disconnected"]["results"].values()
    } == {"SKIPPED"}
    assert payload["disconnected"]["outputs"] == []
    disconnected_operation = result.get_context("disconnected")
    assert result.get_child_operations(disconnected_operation) == []


async def test_flow_executes_only_reverse_dependencies_of_outputs():
    called: list[str] = []

    @durable_dag
    def graph():
        @durable_node
        async def run(name: str, *, fail: bool = False) -> str:
            called.append(name)
            if fail:
                msg = f"{name} must not run"
                raise RuntimeError(msg)
            return name

        active_root = node(run("active-root"), name="active-root")
        selected = node(run("selected"), name="selected")
        active_root >> selected

        node(
            run("unused-root", fail=True),
            name="unused-root",
        )
        unused_descendant = node(
            run("unused-descendant", fail=True),
            name="unused-descendant",
        )
        selected >> unused_descendant
        return selected.outcome

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="pruned-flow")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert called == ["active-root", "selected"]
    payload = json.loads(result.result)
    assert payload["outputs"] == ["selected"]
    assert payload["unhandledFailures"] == []
    assert payload["results"]["unused-root"]["status"] == "SKIPPED"
    assert payload["results"]["unused-descendant"]["status"] == "SKIPPED"

    flow_operation = result.get_context("pruned-flow")
    assert [
        operation.name for operation in result.get_child_operations(flow_operation)
    ] == ["active-root", "selected"]


async def test_all_unmatched_dependencies_skip_downstream_callable():
    called: list[str] = []

    @durable_dag
    def graph():
        @durable_node
        async def succeed() -> str:
            return cast(FlowNodeContext, get_current_context()).operation_name or ""

        @durable_node
        async def skipped() -> None:
            called.append("skipped")

        a = node(succeed(), name="A")
        b = node(succeed(), name="B")
        c = node(skipped(), name="C")
        (a.failed | b.failed) >> c
        return c.result()

    @durable_execution
    async def handler(event):
        return (await flow(graph(), name="unmatched")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert called == []
    assert payload["results"]["C"]["status"] == "SKIPPED"
    assert payload["outputs"][0]["status"] == "SKIPPED"


async def test_nested_flow_can_run_inside_node():
    @durable_dag
    def inner_graph(value: str):
        @durable_node
        async def inner() -> str:
            return value

        return node(inner(), name="inner-node").outcome

    @durable_dag
    def outer_graph():
        @durable_node
        async def outer() -> str:
            inner_result = await flow(inner_graph("nested"), name="inner-flow")
            assert len(inner_result.outputs) == 1
            return cast(str, inner_result.output)

        return node(outer(), name="outer-node").outcome

    @durable_execution
    async def handler(event):
        return (await flow(outer_graph(), name="outer-flow")).to_dict()

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["outputs"] == ["nested"]
    outer_flow = result.get_context("outer-flow")
    outer_node = result.get_child_operations(outer_flow)[0]
    inner_flow = result.get_child_operations(outer_node)[0]
    assert inner_flow.name == "inner-flow"
    assert result.get_child_operations(inner_flow)[0].name == "inner-node"


async def test_flow_owned_node_outcomes_preserve_types_across_replay(monkeypatch):
    monkeypatch.setenv("DURABLE_EXECUTION_TIME_SCALE", "0")
    observed: list[tuple[type[Any], type[Any]]] = []

    @durable_dag
    def inner_graph():
        @durable_node
        async def inner() -> str:
            return "nested"

        return node(inner(), name="inner-node").outcome

    @durable_dag
    def outer_graph():
        @durable_node
        async def return_flow() -> FlowResult:
            return await flow(inner_graph(), name="inner-flow")

        @durable_node
        async def consume(inner_result: FlowResult) -> FlowNodeResult[Any]:
            inner_node_result = inner_result.get_result("inner-node")
            observed.append((type(inner_result), type(inner_node_result)))
            await wait(timedelta(seconds=1), name="consumer-wait")
            return inner_node_result

        nested_flow = node(return_flow(), name="nested-flow")
        return node(
            consume(nested_flow.outcome),
            name="consume",
        ).outcome

    @durable_execution
    async def handler(event):
        result = await flow(outer_graph(), name="outer-flow")
        assert isinstance(result.output, FlowNodeResult)
        return result.output.outcome

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert json.loads(result.result) == "nested"
    assert observed == [
        (FlowResult, FlowNodeResult),
        (FlowResult, FlowNodeResult),
    ]


async def test_recovery_node_can_return_injected_error():
    @durable_dag
    def graph():
        @durable_node
        async def fail() -> None:
            msg = "payment declined"
            raise ValueError(msg)

        @durable_node
        async def recover(error: ErrorObject | None) -> ErrorObject:
            assert error is not None
            return error

        source = node(fail(), name="source")
        return node(recover(source.error), name="recovery").outcome

    @durable_execution
    async def handler(event):
        result = await flow(graph(), name="error-outcome")
        assert isinstance(result.output, ErrorObject)
        return result.output.to_dict()

    async with create_local_runner(handler=handler, input={}, timeout=10) as runner:
        result = await runner.run()

    payload = json.loads(result.result)
    assert payload["ErrorType"] == "ValueError"
    assert payload["ErrorMessage"] == "payment declined"


async def test_operation_ids_are_stable_across_sibling_completion_orders():
    async def run_once(delay_a: float, delay_b: float) -> dict[str, str]:
        @durable_callable
        async def checkpoint(value: str) -> str:
            return value

        @durable_dag
        def graph():
            @durable_node
            async def run_a() -> str:
                await asyncio.sleep(delay_a)
                return await step(checkpoint("A"), name="step-A")

            @durable_node
            async def run_b() -> str:
                await asyncio.sleep(delay_b)
                return await step(checkpoint("B"), name="step-B")

            a = node(run_a(), name="A")
            b = node(run_b(), name="B")
            return a.outcome, b.outcome

        @durable_execution
        async def handler(event):
            return (await flow(graph(), name="stable-flow")).to_dict()

        async with create_local_runner(
            handler=handler,
            input={},
            timeout=10,
        ) as runner:
            result = await runner.run()

        return {
            operation.name: operation.operation_id
            for operation in result.get_all_operations()
            if operation.name is not None
        }

    a_first = await run_once(0, 0.01)
    b_first = await run_once(0.01, 0)

    assert a_first == b_first
    assert set(a_first) == {"stable-flow", "A", "B", "step-A", "step-B"}


async def test_serialization_failure_does_not_activate_failure_route():
    called: list[str] = []

    @durable_dag
    def graph():
        @durable_node
        async def unsupported_result():
            called.append("source")
            return object()

        @durable_node
        async def should_not_run() -> None:
            called.append("handler")

        source = node(unsupported_result(), name="source")
        handler = node(should_not_run(), name="handler")
        source.failed >> handler
        return handler.outcome

    @durable_execution
    async def handler(event):
        await flow(graph(), name="serialization-control")

    async with create_local_runner(
        handler=handler,
        input={},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.FAILED
    assert result.error is not None
    assert result.error.type == "ExecutionError"
    assert called == ["source"]

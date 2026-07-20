"""Tests for declarative acyclic durable flows."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import cast
from unittest.mock import Mock

import pytest

from async_durable_execution import (
    DurableContext,
    ErrorObject,
    FlowDefinitionError,
    FlowExecutionError,
    FlowNodeContext,
    FlowNodeResult,
    FlowNodeStatus,
    InvalidStateError,
    InvocationStatus,
    create_local_runner,
    durable_callable,
    durable_dag,
    durable_execution,
    durable_node,
    flow,
    get_current_context,
    node,
    step,
    wait,
)
from async_durable_execution.context import bind_current_context
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
    def invalid_graph():
        node(return_name(), name="A")
        return "A"

    with bind_current_context(context):
        with pytest.raises(FlowDefinitionError, match="must return"):
            flow(invalid_graph())

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
        return d

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
    assert payload["outputs"] == [
        {"status": "SUCCEEDED", "outcome": "root-B+root-C", "error": None}
    ]
    assert payload["unhandledFailures"] == []

    flow_operation = result.get_context("diamond")
    assert flow_operation.status is OperationStatus.SUCCEEDED
    node_operations = result.get_child_operations(flow_operation)
    assert [operation.name for operation in node_operations] == ["A", "B", "C", "D"]
    assert all(
        operation.status is OperationStatus.SUCCEEDED for operation in node_operations
    )


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
        return d

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


async def test_unhandled_failure_raises_after_flow_result_is_checkpointed():
    captured_result = None

    @durable_dag
    def graph():
        @durable_node
        async def fail() -> None:
            msg = "unhandled"
            raise RuntimeError(msg)

        return node(fail(), name="failure")

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
        return a, b

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
    assert [output["outcome"] for output in payload["outputs"]] == ["A", "B"]
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
        return handler

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
        return handler

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
        return d

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
        return c

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
    assert payload["outputs"][0]["outcome"] == "first+second"


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
        return observer

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
        return b

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
        return recovery

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
        return recovery

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
    assert payload["outputs"][0]["outcome"] == "recovered"
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
        return handler

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
        "unhandledFailures": [],
    }
    assert list(payload["disconnected"]["results"]) == ["A", "B"]
    assert payload["disconnected"]["outputs"] == []


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
        return c

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

        return node(inner(), name="inner-node")

    @durable_dag
    def outer_graph():
        @durable_node
        async def outer() -> str:
            inner_result = await flow(inner_graph("nested"), name="inner-flow")
            assert len(inner_result.outputs) == 1
            return cast(str, inner_result.outputs[0].outcome)

        return node(outer(), name="outer-node")

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
    assert payload["outputs"][0]["outcome"] == "nested"
    outer_flow = result.get_context("outer-flow")
    outer_node = result.get_child_operations(outer_flow)[0]
    inner_flow = result.get_child_operations(outer_node)[0]
    assert inner_flow.name == "inner-flow"
    assert result.get_child_operations(inner_flow)[0].name == "inner-node"


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
            return a, b

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
        return handler

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

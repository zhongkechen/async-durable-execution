"""Graph execution contracts, observed only through the public API."""

import asyncio
import pytest
from async_durable_execution import *


async def execute(graph):
    @durable_execution
    async def handler(event):
        result = await flow(graph())
        return result.to_dict()

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    return FlowResult.from_dict(result.get_deserialized_result())


async def test_chain_and_unreachable_pruning():
    ran = []

    @durable_node
    async def number(value):
        ran.append(value)
        return value

    @durable_node
    async def plus(value):
        return value + 1

    @durable_dag
    def graph():
        a = node(number(4), name="a")
        node(number(999), name="unreachable")
        b = node(plus(a.outcome), name="b")
        return b.outcome

    result = await execute(graph)
    assert result.output == 5
    assert ran == [4]
    assert result.get_result("unreachable").status is FlowNodeStatus.SKIPPED


@pytest.mark.parametrize("projection", ["error", "result", "outcome"])
async def test_output_failure_handling(projection):
    @durable_node
    async def bad():
        raise ValueError("bad")

    @durable_dag
    def graph():
        a = node(bad(), name="bad")
        return getattr(a, projection)

    @durable_execution
    async def handler(event):
        try:
            result = await flow(graph())
            return ("returned", result.has_unhandled_failures)
        except FlowExecutionError as error:
            return ("raised", list(error.result.unavailable_outputs))

    async with create_local_runner(handler=handler) as r:
        result = await r.run()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == (
        ["raised", ["bad"]] if projection == "outcome" else ["returned", False]
    )


async def test_failed_route_and_consumer_argument_isolation():
    @durable_node
    async def source():
        return {"value": 1}

    @durable_node
    async def change(value):
        value["value"] = 9
        return value["value"]

    @durable_node
    async def read(value):
        return value["value"]

    @durable_dag
    def graph():
        a = node(source(), name="a")
        b = node(change(a.outcome), name="b")
        c = node(read(a.outcome), name="c")
        return b.outcome, c.outcome, a.outcome

    result = await execute(graph)
    assert result.output == (9, 1, {"value": 1})


async def test_failed_condition_handles_source_failure():
    @durable_node
    async def bad():
        raise ValueError("bad")

    @durable_node
    async def repair(error):
        return error.message

    @durable_dag
    def graph():
        a = node(bad(), name="bad")
        b = node(repair(a.error), name="repair")
        return b.outcome

    result = await execute(graph)
    assert result.output == "bad"
    assert result.unhandled_failures == ()


async def test_any_choice_is_preserved_when_other_dependency_finishes_on_resume():
    observed = []
    effects = []

    @durable_node
    async def slow():
        await wait(1, name="slow-wait")
        return "slow"

    @durable_node
    async def fast():
        return "fast"

    @durable_node
    async def consume():
        context = get_node_context()
        choices = sorted(context.dependency_results)
        observed.append(choices)

        async def commit():
            effects.append(choices)
            return choices

        selected = await step(commit, name="choose")
        await wait(1, name="consumer-wait")
        return selected

    @durable_dag
    def graph():
        a = node(slow(), name="slow")
        b = node(fast(), name="fast")
        c = node(consume(), name="consumer", dependency=a | b)
        return c.outcome

    result = await execute(graph)
    assert result.output == ["fast"]
    assert all(value == ["fast"] for value in observed)
    assert effects == [["fast"]]


async def test_definition_validation_precedes_side_effects():
    @durable_node
    async def body():
        return 1

    @durable_dag
    def cyclic():
        a = node(body(), name="a")
        b = node(body(), name="b")
        a >> b
        b >> a
        return a.result

    @durable_dag
    def invalid_operation():
        wait(1)

    @durable_execution
    async def handler(event):
        with pytest.raises(FlowDefinitionError):
            flow(cyclic())
        with pytest.raises(InvalidStateError):
            flow(invalid_operation())
        return 1

    async with create_local_runner(handler=handler) as r:
        result = await r.run()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.operations == []


async def test_none_output_executes_nothing():
    called = []

    @durable_node
    async def body():
        called.append(1)

    @durable_dag
    def graph():
        node(body(), name="unused")

    result = await execute(graph)
    assert result.output is None and called == []


async def test_nested_projection_and_multiple_outputs():
    @durable_node
    async def source():
        return 7

    @durable_node
    async def sink(value):
        return value["values"][0] + 1

    @durable_dag
    def graph():
        a = node(source(), name="a")
        b = node(sink({"values": [a.outcome]}), name="b")
        return a.outcome, b.outcome

    assert (await execute(graph)).output == (7, 8)

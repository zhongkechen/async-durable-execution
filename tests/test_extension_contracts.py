"""Custom operations must respect reservation identity and owning scopes."""

import pytest

from async_durable_execution import (
    ExtensionStepResult,
    InvocationStatus,
    OperationSubType,
    create_local_runner,
    durable_execution,
    get_extension_context,
    run_in_child_context,
    wait,
)


@pytest.mark.parametrize(
    "name, error", [(42, TypeError), ("", ValueError), ("  ", ValueError)]
)
async def test_invalid_reservation_name_does_not_create_an_operation(name, error):
    @durable_execution
    async def handler(event):
        with pytest.raises(error, match="name must"):
            get_extension_context().reserve(name)

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_all_operations() == []


@pytest.mark.parametrize("local_id", [42, "", "  "])
async def test_invalid_local_identity_does_not_create_an_operation(local_id):
    @durable_execution
    async def handler(event):
        with pytest.raises(ValueError, match="nonblank string"):
            get_extension_context().reserve(local_operation_id=local_id)

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_all_operations() == []


async def test_local_identity_cannot_be_reused_or_reserved_after_selection():
    calls = []

    async def work(state):
        calls.append("work")
        return ExtensionStepResult.succeed("saved")

    @durable_execution
    async def handler(event):
        extension = get_extension_context()
        reserved = extension.reserve("work", local_operation_id="unique")
        with pytest.raises(ValueError, match="already reserved"):
            extension.reserve(local_operation_id="unique")
        result = await reserved.step(work, sub_type="CustomStep")
        with pytest.raises(RuntimeError, match="before selecting"):
            extension.reserve(local_operation_id="late")
        with pytest.raises(RuntimeError, match="only be selected once"):
            reserved.step(work, sub_type="CustomStep")
        await wait(1)
        return result

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == "saved"
    assert calls == ["work"]


async def test_child_cannot_reserve_or_select_through_parent_extension():
    effects = []

    async def work(state):
        effects.append("parent")
        return ExtensionStepResult.succeed("parent-result")

    @durable_execution
    async def handler(event):
        parent = get_extension_context()
        reserved = parent.reserve("parent-work", local_operation_id="work")

        async def child():
            with pytest.raises(RuntimeError, match="owning scope"):
                parent.reserve("illegal-child-work")
            with pytest.raises(RuntimeError, match="another durable context"):
                reserved.step(work, sub_type="CustomStep")
            return "child-result"

        child_result = await run_in_child_context(child, name="child")
        parent_result = await reserved.step(work, sub_type="CustomStep")
        await wait(1)
        return child_result, parent_result

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == ["child-result", "parent-result"]
    assert effects == ["parent"]
    assert result.get_child_operations(result.get_context("child")) == []


@pytest.mark.parametrize("subtype", [None, 42, "", "  ", OperationSubType.STEP.value])
async def test_invalid_subtype_does_not_consume_reservation(subtype):
    calls = []

    async def work(state):
        calls.append("work")
        return ExtensionStepResult.succeed(7)

    @durable_execution
    async def handler(event):
        reserved = get_extension_context().reserve("work")
        with pytest.raises(ValueError, match="sub_type|subtypes"):
            reserved.step(work, sub_type=subtype)
        value = await reserved.step(work, sub_type=OperationSubType.STEP)
        await wait(1)
        return value

    async with create_local_runner(handler=handler, timeout=3) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert result.get_deserialized_result() == 7
    assert calls == ["work"]

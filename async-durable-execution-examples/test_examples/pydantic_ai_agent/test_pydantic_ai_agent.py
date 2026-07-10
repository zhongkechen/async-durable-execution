"""Tests for the async-native Pydantic AI durable capability prototype."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from async_durable_execution import InvocationStatus
from async_durable_execution import JsonSerDes
from async_durable_execution_examples.pydantic_ai_agent import pydantic_ai_agent


@dataclass(frozen=True)
class FakeRunContext:
    run_step: int = 3


@dataclass(frozen=True)
class FakeToolCall:
    tool_name: str = "lookup_customer_tier"
    tool_call_id: str = "call_123"


@dataclass(frozen=True)
class FakeToolDefinition:
    name: str = "lookup_customer_tier"


async def test_model_requests_are_plain_async_durable_steps(monkeypatch):
    calls: list[dict[str, Any]] = []
    handler_loop: asyncio.AbstractEventLoop | None = None

    async def fake_step(func, *, name=None, serdes=None, **kwargs):
        calls.append({"name": name, "serdes": serdes, "kwargs": kwargs})
        return await func()

    monkeypatch.setattr(pydantic_ai_agent, "step", fake_step)

    async def handler(request_context: dict[str, str]) -> dict[str, str]:
        nonlocal handler_loop
        handler_loop = asyncio.get_running_loop()
        await asyncio.sleep(0)
        return {"response": request_context["prompt"]}

    capability = pydantic_ai_agent.AsyncLambdaDurability(agent_name="support")
    original_loop = asyncio.get_running_loop()

    result = await capability.wrap_model_request(
        FakeRunContext(),
        request_context={"prompt": "hello"},
        handler=handler,
    )

    assert result == {"response": "hello"}
    assert handler_loop is original_loop
    assert calls == [
        {
            "name": "support.model.request.3",
            "serdes": capability.model_step_serdes,
            "kwargs": {},
        }
    ]


async def test_tool_execution_uses_tool_specific_serdes(monkeypatch):
    calls: list[dict[str, Any]] = []
    lookup_serdes = JsonSerDes()

    async def fake_step(func, *, name=None, serdes=None, **kwargs):
        calls.append({"name": name, "serdes": serdes, "kwargs": kwargs})
        return await func()

    monkeypatch.setattr(pydantic_ai_agent, "step", fake_step)

    async def handler(args: dict[str, str]) -> str:
        await asyncio.sleep(0)
        return f"tier:{args['customer_id']}"

    capability = pydantic_ai_agent.AsyncLambdaDurability(
        agent_name="support",
        tool_step_serdes_by_name={"lookup_customer_tier": lookup_serdes},
    )

    result = await capability.wrap_tool_execute(
        FakeRunContext(),
        call=FakeToolCall(),
        tool_def=FakeToolDefinition(),
        args={"customer_id": "customer-123"},
        handler=handler,
    )

    assert result == "tier:customer-123"
    assert calls == [
        {
            "name": "support.tool.lookup_customer_tier.3.call_123",
            "serdes": lookup_serdes,
            "kwargs": {},
        }
    ]


async def test_wrap_run_sets_sequential_tool_execution_mode(monkeypatch):
    capability = pydantic_ai_agent.AsyncLambdaDurability()
    modes: list[str] = []

    @contextmanager
    def fake_execution_mode(mode: str):
        modes.append(f"enter:{mode}")
        try:
            yield
        finally:
            modes.append(f"exit:{mode}")

    monkeypatch.setattr(
        pydantic_ai_agent,
        "_parallel_tool_call_execution_mode",
        fake_execution_mode,
    )

    async def handler() -> str:
        modes.append("handler")
        return "done"

    assert await capability.wrap_run(None, handler=handler) == "done"
    assert modes == ["enter:sequential", "handler", "exit:sequential"]


async def test_model_response_serdes_falls_back_to_json():
    serdes = pydantic_ai_agent.PydanticAIModelResponseSerDes()
    payload = {
        "tool": {
            "name": "lookup_customer_tier",
            "args": {"customer_id": "enterprise-123"},
            "call_id": "call-lookup-tier",
        }
    }

    serialized = await serdes.serialize(payload)

    assert await serdes.deserialize(serialized) == payload


async def test_support_agent_checkpoints_model_and_tool_steps(durable_runner):
    async with durable_runner(
        handler=pydantic_ai_agent.handler,
        input={
            "customer_id": "enterprise-123",
            "prompt": "Should we escalate this production incident?",
        },
        timeout=30,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = result.get_deserialized_result()
    assert result_data == {
        "customer_id": "enterprise-123",
        "answer": {
            "support_advice": (
                "Escalate to the enterprise support queue and include recent "
                "incident details."
            ),
            "escalate": True,
            "confidence": 0.93,
        },
    }

    operation_names = {operation.name for operation in result.get_all_operations()}
    assert "lambda_support_agent.model.request.0" in operation_names
    assert "lambda_support_agent.tool.lookup_customer_tier.0.call-lookup-tier" in (
        operation_names
    )
    assert "lambda_support_agent.model.request.1" in operation_names

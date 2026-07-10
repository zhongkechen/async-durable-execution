"""Async-native durable capability prototype for Pydantic AI-style agents.

The official SDK prototype needs a sync/async bridge so Pydantic AI can run on
its own event loop while synchronous durable steps run on a handler thread. This
SDK supports async durable operations directly, so capability hooks can simply
await durable steps from the agent loop.
"""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable, Generator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Generic, TypeVar

from async_durable_execution import SerDes
from async_durable_execution import durable_callable
from async_durable_execution import durable_execution
from async_durable_execution import step


T = TypeVar("T")

try:
    from pydantic_ai.capabilities import AbstractCapability  # type: ignore[import-not-found]
except ImportError:

    class AbstractCapability(Generic[T]):  # type: ignore[no-redef]
        """Fallback base so this example remains importable without Pydantic AI."""


@dataclass(frozen=True)
class SupportDependencies:
    """Dependencies passed to the support agent."""

    customer_id: str


@dataclass(frozen=True)
class SupportAnswer:
    """Structured support answer returned by the prototype agent."""

    support_advice: str
    escalate: bool
    confidence: float


@dataclass(frozen=True)
class PrototypeAgentResult:
    """Small stand-in for Pydantic AI's run result object."""

    output: SupportAnswer


@dataclass(frozen=True)
class _RunContext:
    run_step: int


@dataclass(frozen=True)
class _ToolCall:
    tool_name: str
    tool_call_id: str


@dataclass(frozen=True)
class _ToolDefinition:
    name: str


@dataclass(frozen=True)
class _PydanticResponseAdapter:
    adapter: Any
    response_type: type[Any]


class PydanticAIModelResponseSerDes(SerDes[Any]):
    """Serialize Pydantic AI model responses, with JSON fallback for tests."""

    async def serialize(self, value: Any) -> str:
        response_adapter = self._response_adapter_or_none()
        if response_adapter is not None and isinstance(
            value,
            response_adapter.response_type,
        ):
            return response_adapter.adapter.dump_json(value).decode("utf-8")

        if hasattr(value, "model_dump_json"):
            return str(value.model_dump_json())

        return json.dumps(value, default=_json_default)

    async def deserialize(self, data: str) -> Any:
        response_adapter = self._response_adapter_or_none()
        if response_adapter is not None:
            try:
                return response_adapter.adapter.validate_json(data)
            except Exception:
                pass

        return json.loads(data)

    @staticmethod
    def _response_adapter_or_none() -> _PydanticResponseAdapter | None:
        try:
            import pydantic  # type: ignore[import-not-found]
            from pydantic_ai.messages import ModelResponse  # type: ignore[import-not-found]
        except ImportError:
            return None

        return _PydanticResponseAdapter(
            adapter=pydantic.TypeAdapter(ModelResponse),
            response_type=ModelResponse,
        )


def _json_default(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")

    msg = f"Object of type {type(value).__name__} is not JSON serializable"
    raise TypeError(msg)


@contextmanager
def _parallel_tool_call_execution_mode(mode: str) -> Generator[None]:
    """Enter Pydantic AI's tool execution mode when the package is installed."""

    try:
        from pydantic_ai.tool_manager import ToolManager  # type: ignore[import-not-found]
    except ImportError:
        yield
        return

    parallel_execution_mode = getattr(ToolManager, "parallel_execution_mode", None)
    if parallel_execution_mode is None:
        msg = "Pydantic AI ToolManager.parallel_execution_mode is unavailable"
        raise RuntimeError(msg)

    with parallel_execution_mode(mode):
        yield


def _step_name_part(value: Any, *, default: str = "unknown") -> str:
    if value is None:
        return default
    name = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value)).strip("-")
    return name or default


class AsyncLambdaDurability(AbstractCapability[Any]):
    """Pydantic AI capability that checkpoints model and tool calls as steps."""

    def __init__(
        self,
        *,
        agent_name: str = "pydantic-ai-agent",
        model_step_serdes: SerDes[Any] | None = None,
        tool_step_serdes: SerDes[Any] | None = None,
        tool_step_serdes_by_name: dict[str, SerDes[Any] | None] | None = None,
        sequential_tool_calls: bool = True,
    ) -> None:
        self.agent_name = agent_name
        self.model_step_serdes = model_step_serdes or PydanticAIModelResponseSerDes()
        self.tool_step_serdes = tool_step_serdes
        self.tool_step_serdes_by_name = tool_step_serdes_by_name or {}
        self.sequential_tool_calls = sequential_tool_calls

        self.id = f"{_step_name_part(agent_name)}-async-lambda-durability"
        self.description = "Checkpoint Pydantic AI model requests and tool calls"
        self.defer_loading = False

    @classmethod
    def get_serialization_name(cls) -> str | None:
        return None

    async def wrap_run(self, ctx: Any, *, handler: Callable[[], Awaitable[T]]) -> T:
        if not self.sequential_tool_calls:
            return await handler()

        # Durable operation ids are allocated by call order, so tool steps must
        # not race each other within one model response.
        with _parallel_tool_call_execution_mode("sequential"):
            return await handler()

    async def wrap_model_request(
        self,
        ctx: Any,
        *,
        request_context: Any,
        handler: Callable[[Any], Awaitable[T]],
    ) -> T:
        @durable_callable
        async def run_model_request() -> T:
            return await handler(request_context)

        return await step(
            run_model_request(),
            name=self._model_step_name(ctx),
            serdes=self.model_step_serdes,
        )

    async def wrap_tool_execute(
        self,
        ctx: Any,
        *,
        call: Any,
        tool_def: Any,
        args: dict[str, Any],
        handler: Callable[[dict[str, Any]], Awaitable[T]],
    ) -> T:
        tool_name = self._tool_name(call=call, tool_def=tool_def)

        @durable_callable
        async def run_tool() -> T:
            return await handler(args)

        return await step(
            run_tool(),
            name=self._tool_step_name(ctx=ctx, call=call, tool_name=tool_name),
            serdes=self.tool_step_serdes_by_name.get(
                tool_name,
                self.tool_step_serdes,
            ),
        )

    def _model_step_name(self, ctx: Any) -> str:
        run_step = _step_name_part(getattr(ctx, "run_step", None), default="0")
        return f"{self.agent_name}.model.request.{run_step}"

    def _tool_step_name(self, *, ctx: Any, call: Any, tool_name: str) -> str:
        run_step = _step_name_part(getattr(ctx, "run_step", None), default="0")
        call_id = _step_name_part(getattr(call, "tool_call_id", None), default="call")
        tool_name_part = _step_name_part(tool_name, default="tool")
        return f"{self.agent_name}.tool.{tool_name_part}.{run_step}.{call_id}"

    @staticmethod
    def _tool_name(*, call: Any, tool_def: Any) -> str:
        return str(
            getattr(tool_def, "name", None)
            or getattr(call, "tool_name", None)
            or "tool"
        )


LambdaDurability = AsyncLambdaDurability
LambdaDurableExecutionCapability = AsyncLambdaDurability


async def _lookup_customer_tier(customer_id: str) -> str:
    """Return the support tier for the current customer."""

    return "enterprise" if customer_id.startswith("enterprise-") else "standard"


def create_pydantic_support_agent(
    model: Any,
    *,
    agent_name: str = "lambda_support_agent",
) -> Any:
    """Create a real Pydantic AI agent when Pydantic AI is installed."""

    try:
        from pydantic_ai import Agent, RunContext  # type: ignore[import-not-found]
    except ImportError as exc:
        msg = "The pydantic-ai package is required to create a real Pydantic AI agent."
        raise RuntimeError(msg) from exc

    agent = Agent(
        model,
        name=agent_name,
        deps_type=SupportDependencies,
        output_type=SupportAnswer,
        instructions=(
            "You are a concise first-tier support agent. Use customer tier "
            "information when it is relevant, decide whether to escalate to a "
            "human, and provide a confidence score from 0 to 1."
        ),
        capabilities=[AsyncLambdaDurability(agent_name=agent_name)],
    )

    @agent.tool
    async def lookup_customer_tier(ctx: RunContext[SupportDependencies]) -> str:
        """Return the support tier for the current customer."""

        return await _lookup_customer_tier(ctx.deps.customer_id)

    return agent


async def _support_model_request(request_context: dict[str, Any]) -> dict[str, Any]:
    """Small deterministic model stand-in for the runnable example."""

    prompt = str(request_context["prompt"])
    customer_tier = request_context.get("customer_tier")

    if customer_tier is None:
        return {
            "tool": {
                "name": "lookup_customer_tier",
                "args": {"customer_id": request_context["customer_id"]},
                "call_id": "call-lookup-tier",
            }
        }

    lower_prompt = prompt.lower()
    should_escalate = customer_tier == "enterprise" and any(
        word in lower_prompt for word in ("incident", "outage", "production", "sev")
    )
    advice = (
        "Escalate to the enterprise support queue and include recent incident details."
        if should_escalate
        else "Answer with standard troubleshooting guidance and monitor for changes."
    )

    return {
        "output": {
            "support_advice": advice,
            "escalate": should_escalate,
            "confidence": 0.93 if should_escalate else 0.78,
        }
    }


class _PrototypeSupportAgent:
    def __init__(self, capability: AsyncLambdaDurability) -> None:
        self.capability = capability

    async def run(
        self,
        prompt: str,
        *,
        deps: SupportDependencies,
    ) -> PrototypeAgentResult:
        async def run_agent() -> PrototypeAgentResult:
            first_response = await self.capability.wrap_model_request(
                _RunContext(run_step=0),
                request_context={
                    "prompt": prompt,
                    "customer_id": deps.customer_id,
                    "customer_tier": None,
                },
                handler=_support_model_request,
            )

            tool_request = first_response.get("tool")
            if tool_request is not None:
                customer_tier = await self.capability.wrap_tool_execute(
                    _RunContext(run_step=0),
                    call=_ToolCall(
                        tool_name=str(tool_request["name"]),
                        tool_call_id=str(tool_request["call_id"]),
                    ),
                    tool_def=_ToolDefinition(name=str(tool_request["name"])),
                    args=tool_request["args"],
                    handler=lambda args: _lookup_customer_tier(
                        str(args["customer_id"]),
                    ),
                )
                final_response = await self.capability.wrap_model_request(
                    _RunContext(run_step=1),
                    request_context={
                        "prompt": prompt,
                        "customer_id": deps.customer_id,
                        "customer_tier": customer_tier,
                    },
                    handler=_support_model_request,
                )
            else:
                final_response = first_response

            output = final_response["output"]
            return PrototypeAgentResult(
                output=SupportAnswer(
                    support_advice=str(output["support_advice"]),
                    escalate=bool(output["escalate"]),
                    confidence=float(output["confidence"]),
                )
            )

        return await self.capability.wrap_run(None, handler=run_agent)


def create_support_agent() -> _PrototypeSupportAgent:
    """Create the dependency-free agent used by this runnable prototype."""

    return _PrototypeSupportAgent(
        AsyncLambdaDurability(agent_name="lambda_support_agent")
    )


def _parse_event(event: Any) -> tuple[str, SupportDependencies]:
    if isinstance(event, dict):
        prompt = event.get("prompt")
        customer_id = event.get("customer_id", "anonymous")
    else:
        prompt = str(event)
        customer_id = "anonymous"

    if not isinstance(prompt, str) or not prompt.strip():
        msg = "event must include a non-empty 'prompt' string"
        raise ValueError(msg)

    if not isinstance(customer_id, str) or not customer_id.strip():
        msg = "event 'customer_id' must be a non-empty string when provided"
        raise ValueError(msg)

    return prompt, SupportDependencies(customer_id=customer_id)


@durable_execution
async def handler(event: Any) -> dict[str, Any]:
    """Durable support agent prototype with checkpointed model and tool calls."""

    prompt, deps = _parse_event(event)
    result = await create_support_agent().run(prompt, deps=deps)

    return {
        "customer_id": deps.customer_id,
        "answer": asdict(result.output),
    }

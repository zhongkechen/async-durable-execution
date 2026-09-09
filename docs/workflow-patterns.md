# Workflow Patterns

These patterns build on the [execution model](index.md#execution-model).
Workflow control flow and in-memory state are reconstructed during replay.
External calls and other nondeterministic work belong in named steps, and each
loop iteration must produce the same operation names for the same saved history.

## Agentic Loop

Checkpoint model and tool calls separately. Rebuilding `messages` from the
input and saved step results on every replay is safe because each mutation is
deterministic.

```python
from typing import Any

from async_durable_execution import durable_callable, durable_execution, step


@durable_callable
async def invoke_model(messages: list[dict[str, Any]]) -> dict[str, Any]:
    return await model_client.invoke(messages)


@durable_callable
async def run_tool(tool: dict[str, Any]) -> dict[str, Any]:
    return await tool_registry.execute(tool)


@durable_execution
async def handler(event: dict[str, Any]) -> str:
    messages: list[dict[str, Any]] = [
        {"role": "user", "content": str(event["prompt"])}
    ]

    for iteration in range(20):
        response = await step(
            invoke_model(messages),
            name=f"invoke-model-{iteration}",
        )
        messages.append(
            {"role": "assistant", "content": response["response"]}
        )

        tool = response.get("tool")
        if tool is None:
            return str(response["response"])

        tool_result = await step(
            run_tool(tool),
            name=f"run-tool-{iteration}",
        )
        messages.append(
            {
                "role": "tool",
                "name": tool["name"],
                "content": tool_result,
            }
        )

    raise RuntimeError("Agent reached the maximum number of iterations")
```

Use an iteration number or another replay-stable identifier in operation names.
Set an explicit iteration limit so a faulty model cannot grow execution history
without bound.

## Human Approval

`wait_for_callback()` creates a callback, runs its submitter as a step, and
suspends the execution until an external system reports success or failure.

```python
from datetime import timedelta
from typing import Any

from async_durable_execution import (
    durable_callable,
    durable_execution,
    get_wait_for_callback_context,
    step,
    wait_for_callback,
)


@durable_callable
async def generate_plan(event: dict[str, Any]) -> dict[str, Any]:
    return await planner.create(event)


@durable_callable
async def submit_approval(
    approver_email: str,
    plan: dict[str, Any],
) -> None:
    context = get_wait_for_callback_context()
    await approval_service.send(
        approver_email=approver_email,
        plan=plan,
        callback_id=context.callback_id,
    )


@durable_callable
async def perform_action(plan: dict[str, Any]) -> None:
    await action_service.execute(plan)


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, str]:
    plan = await step(generate_plan(event), name="generate-plan")
    answer = await wait_for_callback(
        submit_approval(str(event["approver_email"]), plan),
        timeout=timedelta(hours=24),
        name="wait-for-approval",
    )

    if answer != "APPROVED":
        return {"status": "rejected"}

    await step(perform_action(plan), name="perform-action")
    return {"status": "completed"}
```

The submitter can retry. Use the callback ID as an idempotency key when sending
the approval request. The external system needs the callback permissions listed
in [Deploy and Invoke](deployment.md#iam-permissions).

## Saga Compensation

Use `terminal_scope()` to register reverse-order compensation after each
forward operation succeeds. The scope reconstructs registrations during replay,
runs compensation only after failure, and runs cleanup after both success and
failure. Suspension does not trigger either phase.

```python
from typing import Any

from async_durable_execution import (
    DurableTerminalActions,
    durable_callable,
    durable_execution,
    step,
    terminal_scope,
)


@durable_callable
async def book_flight(request: dict[str, Any]) -> None:
    await flight_service.book(request)


@durable_callable
async def book_hotel(request: dict[str, Any]) -> None:
    await hotel_service.book(request)


@durable_callable
async def cancel_flight(request: dict[str, Any]) -> None:
    await flight_service.cancel(request)


@durable_callable
async def cancel_hotel(request: dict[str, Any]) -> None:
    await hotel_service.cancel(request)


async def book_trip(
    event: dict[str, Any],
    terminal: DurableTerminalActions,
) -> dict[str, bool]:
    await step(book_flight(event), name="book-flight")
    terminal.compensate(cancel_flight(event), name="cancel-flight")

    await step(book_hotel(event), name="book-hotel")
    terminal.compensate(cancel_hotel(event), name="cancel-hotel")
    return {"success": True}


@durable_execution
async def handler(event: dict[str, Any]) -> dict[str, bool]:
    async def scoped(terminal: DurableTerminalActions) -> dict[str, bool]:
        return await book_trip(event, terminal)

    return await terminal_scope(scoped, name="book-trip")
```

Prefer idempotent booking and cancellation APIs. A remote side effect can
succeed immediately before its step checkpoint fails, so applications that
cannot tolerate ambiguity need service-specific idempotency keys or
reconciliation.

See [Durable Terminal Scopes](terminal-scopes.md) for cleanup, retries,
cancellation policy, nested scopes, and the `finally` hazard.

For simpler sequential workflows, start with the
[getting-started example](getting-started.md#create-a-workflow). For static
acyclic workflows with conditional failure routes, use
[flow and DAG workflows](api/extension/flow.md).

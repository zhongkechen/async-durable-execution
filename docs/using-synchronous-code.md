# Using Synchronous Code

This SDK requires executable workflow bodies such as durable handlers, steps,
and flow nodes to be `async def`, but your business code does not have to be
fully async. You can call existing synchronous functions from async durable
callables as long as you preserve the durable execution replay rules.

Declarative and configuration hooks are intentionally synchronous. For example,
a `@durable_dag` definition must be a regular `def` because it only declares
and validates a flow graph, while its `@durable_node` bodies must be
`async def`.

The important distinction is where the synchronous code runs:

- Deterministic, side-effect-free synchronous code may run directly in the handler.
- Nondeterministic synchronous code must run inside a durable step.
- Blocking synchronous I/O should usually run in a worker thread with `asyncio.to_thread()`.

## What Must Be Async

The SDK validates user-provided durable callables. These entry points must be
asynchronous:

- `@durable_execution` handlers
- `@durable_callable` step functions
- `@durable_node` flow node functions
- child context functions passed to `run_in_child_context()`
- callback submitters passed to `wait_for_callback()`
- item functions passed to `map()`
- branch callables passed to `parallel()`
- condition checks passed to `wait_for_condition()`

Do not pass a regular `def` function directly to `step()`, `run_in_child_context()`,
`node()`, `map()`, or `parallel()`. Wrap synchronous work in an async durable
callable instead.

## What Must Stay Synchronous

These declarative and configuration hooks must be regular synchronous callables,
not `async def` functions:

- `@durable_dag` definitions
- retry and polling strategies
- custom completion callbacks passed to `CompletionConfig.custom()`
- `item_namer` callbacks passed to `map()`
- `summary_generator` callbacks

Keep DAG definitions and structural or metadata hooks deterministic and
side-effect free. They must not perform I/O, start durable operations, or return
awaitables.

## Calling Pure Synchronous Helpers

Pure deterministic helpers can be called directly from your async handler. This is safe
when the function returns the same output for the same input and has no side effects.

```python
from async_durable_execution import durable_callable, durable_execution, step


def normalize_order_id(raw_order_id: str) -> str:
    return raw_order_id.strip().upper()


@durable_callable
async def create_receipt(order_id: str) -> dict:
    return {"receipt_id": f"receipt-{order_id}"}


@durable_execution
async def handler(event: dict) -> dict:
    order_id = normalize_order_id(event["order_id"])
    receipt = await step(create_receipt(order_id), name="create-receipt")
    return {"order_id": order_id, "receipt": receipt}
```

Do not call synchronous helpers directly from the handler if they read clocks, generate
random values, make network calls, query databases, write files, send messages, or
otherwise depend on external state. Those operations can repeat on replay unless they
are inside a step.

## Wrapping Blocking I/O in a Step

For existing synchronous clients such as `requests`, `boto3`, database drivers, or
legacy SDKs, keep the side effect inside a step and run the blocking call through
`asyncio.to_thread()`.

```python
import asyncio
import requests

from async_durable_execution import durable_callable, durable_execution, step


def fetch_customer_sync(customer_id: str) -> dict:
    response = requests.get(
        f"https://example.com/customers/{customer_id}",
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


@durable_callable
async def fetch_customer(customer_id: str) -> dict:
    return await asyncio.to_thread(fetch_customer_sync, customer_id)


@durable_execution
async def handler(event: dict) -> dict:
    customer = await step(
        fetch_customer(event["customer_id"]),
        name="fetch-customer",
    )
    return {"customer": customer}
```

`asyncio.to_thread()` prevents the blocking call from stopping the event loop while the
step is running. The durable step still controls checkpointing, retries, and replay.

## Calling Fast Synchronous Code in a Step

If the synchronous function is CPU-light and does not block on I/O, you can call it
directly inside the async step wrapper.

```python
from async_durable_execution import durable_callable, step


def calculate_tax_sync(order: dict) -> dict:
    return {"amount": round(order["subtotal"] * 0.0825, 2)}


@durable_callable
async def calculate_tax(order: dict) -> dict:
    return calculate_tax_sync(order)


tax = await step(calculate_tax(order), name="calculate-tax")
```

For expensive CPU-bound work, consider moving it to another service or Lambda function
and invoking it durably. Worker threads avoid blocking the event loop, but they do not
make CPU-heavy Python code parallel under the GIL.

## Synchronous Side Effects

Treat synchronous side effects the same way you treat async side effects: put them in
steps and make them idempotent where possible.

```python
import asyncio
import smtplib
from email.message import EmailMessage

from async_durable_execution import durable_callable, durable_execution, step


def send_email_sync(to_address: str, subject: str, body: str) -> None:
    message = EmailMessage()
    message["To"] = to_address
    message["Subject"] = subject
    message.set_content(body)

    with smtplib.SMTP("smtp.example.com", timeout=10) as client:
        client.send_message(message)


@durable_callable
async def send_email(to_address: str, subject: str, body: str) -> None:
    await asyncio.to_thread(send_email_sync, to_address, subject, body)


@durable_execution
async def handler(event: dict) -> dict:
    await step(
        send_email(
            event["to"],
            "Order received",
            f"Order {event['order_id']} was received.",
        ),
        name="send-email",
    )
    return {"status": "sent"}
```

For non-idempotent side effects, use `StepSemantics.AT_MOST_ONCE_PER_RETRY` when
appropriate, and use application-level idempotency keys when the downstream system
supports them.

## Synchronous Callback Submitters

`wait_for_callback()` submitters must be async, but the notification code they call can
be synchronous. Keep the external notification inside the submitter and use
`asyncio.to_thread()` if it blocks.

```python
import asyncio
from datetime import timedelta

from async_durable_execution import (
    WaitForCallbackContext,
    durable_callable,
    durable_execution,
    get_current_context,
    wait_for_callback,
)


def submit_approval_sync(callback_id: str, approver_email: str) -> None:
    send_approval_email(approver_email, callback_id)


@durable_callable
async def submit_approval(approver_email: str) -> None:
    callback_context = get_current_context()
    assert isinstance(callback_context, WaitForCallbackContext)
    await asyncio.to_thread(
        submit_approval_sync,
        callback_context.callback_id,
        approver_email,
    )


@durable_execution
async def handler(event: dict) -> dict:
    approval = await wait_for_callback(
        submit_approval(event["approver_email"]),
        timeout=timedelta(hours=24),
        name="approval",
    )
    return {"approval": approval}
```

## Avoid Nested Event Loops

Do not call `asyncio.run()` inside a durable handler or durable callable. The
`@durable_execution` wrapper owns the event loop for the invocation.

If you have an async library, await it normally from your async durable callable. If you
have a synchronous library, call it directly or use `asyncio.to_thread()` depending on
whether it blocks.

## Checklist

1. Keep executable workflow bodies as `async def`.
2. Keep declarative and configuration hooks synchronous; keep DAG definitions
   and structural or metadata hooks deterministic and side-effect free.
3. Call deterministic synchronous helpers directly only when they have no side effects.
4. Put synchronous I/O, external reads, and writes inside `@durable_callable` steps.
5. Use `asyncio.to_thread()` for blocking synchronous I/O.
6. Do not perform durable operations from inside a step.
7. Do not call `asyncio.run()` from durable code.
8. Name the step that wraps each important synchronous operation so tests and logs stay
   clear.

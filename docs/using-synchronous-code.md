# Using Synchronous Code

Durable handlers and flow nodes must be written with `async def`. Other
executable workflow bodies may use either `def` or `async def`. The SDK awaits
asynchronous callables on the event loop and dispatches synchronous callables
to the loop's worker-thread executor. The active durable context is copied into
that thread, so context getters continue to work.

Declarative and configuration hooks are intentionally synchronous. For example,
a `@durable_dag` definition must be a regular `def` because it only declares
and validates a flow graph. Unlike executable sync callables, these hooks run
directly and are not sent to the thread executor.

The important distinctions are where synchronous code runs and whether it
needs to await durable operations:

- A sync step, child context, callback submitter, map item, parallel branch,
  condition check, or serializer runs in a worker thread.
- Nondeterministic synchronous code and side effects must still run inside a
  durable step.
- Durable operations return `asyncio.Task` objects. Use `async def` for any
  callable that needs to await or compose them.
- Synchronous configuration hooks remain deterministic and side-effect free.

## Async Context Functions and Sync or Async Callables

Functions decorated with `@durable_execution` must use `async def`, including
handlers that do not create durable operations. This keeps the workflow entry
point consistent and ready to compose durable operations.

Functions decorated with `@durable_node` must also use `async def`. Every flow
node runs as a durable child context and may compose child operations.

Functions decorated with `@durable_callable` must use `async def`. Use
`@durable_step` to bind arguments to a synchronous step function.

These other executable entry points accept either `def` or `async def`:

- child context functions passed to `run_in_child_context()`
- callback submitters passed to `wait_for_callback()`
- item functions passed to `map()`
- branch callables passed to `parallel()`
- condition checks passed to `wait_for_condition()`
- `SyncSerDes.serialize()` and `SyncSerDes.deserialize()` methods

Callable parameters can be plain functions or bound methods. `@durable_step`
supports synchronous instance, class, and static methods.
`@durable_callable` and `@durable_node` support the same method forms but
require async implementations. The SDK automatically wraps supported
synchronous callables with `asyncio.to_thread()`.

An async callable runs on the event loop and can await durable operations. A
sync callable runs in a worker thread and should return an ordinary value.

## Composition Functions Must Be Async

Synchronous executable callables are leaf functions. They may use their
scope-specific context getter, call ordinary synchronous helpers, and return an
ordinary value, but they cannot create durable operations. This rule applies
to child contexts, map items, parallel branches, callback submitters, condition
checks, and serializers. Handlers and flow nodes are always async.

Async serializers subclass `SerDes`; synchronous serializers subclass
`SyncSerDes`. Operation APIs accept either interface, while `SerDes` keeps its
awaitable method contract for generic async integrations.

Use `async def` for any function that calls `step()`, `wait()`, `invoke()`,
`recurse()`, `run_in_child_context()`, `flow()`, `map()`, `parallel()`,
`wait_for_callback()`, `wait_for_condition()`, `with_retry()`, or another
durable operation. Returning an async helper from a sync callable does not
bypass this rule. The SDK raises `InvalidStateError` and identifies the
operation when a synchronous user callable attempts durable composition.

An async child context can compose sync and async steps:

```python
from async_durable_execution import (
    durable_callable,
    durable_execution,
    durable_step,
    run_in_child_context,
    step,
)


@durable_step
def blocking_lookup(order_id: str) -> dict:
    return legacy_client.lookup(order_id)


@durable_callable
async def enrich_order(order: dict) -> dict:
    return await async_client.enrich(order)


@durable_callable
async def process_order(order_id: str) -> dict:
    order = await step(blocking_lookup(order_id), name="lookup")
    return await step(enrich_order(order), name="enrich")


@durable_execution
async def handler(event: dict) -> dict:
    return await run_in_child_context(
        process_order(event["order_id"]),
        name="process-order",
    )
```

Here, `blocking_lookup()` runs in a worker thread and `enrich_order()` runs on
the event loop. `process_order()` and `handler()` must be async because they
compose durable operations.

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

Pure deterministic helpers can still be called directly from an async handler.
This is safe when the function returns the same output for the same input, has
no side effects, and does not block.

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

## Blocking I/O in a Sync Step

For existing synchronous clients such as `requests`, `boto3`, database drivers, or
legacy SDKs, keep the side effect inside a synchronous step. The SDK runs the
step function in a worker thread automatically.

```python
import requests

from async_durable_execution import durable_execution, durable_step, step


def fetch_customer_sync(customer_id: str) -> dict:
    response = requests.get(
        f"https://example.com/customers/{customer_id}",
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


@durable_step
def fetch_customer(customer_id: str) -> dict:
    return fetch_customer_sync(customer_id)


@durable_execution
async def handler(event: dict) -> dict:
    customer = await step(
        fetch_customer(event["customer_id"]),
        name="fetch-customer",
    )
    return {"customer": customer}
```

The worker thread prevents the blocking call from stopping the event loop while
the durable step still controls checkpointing, retries, and replay.

## Calling Fast Synchronous Code in a Step

CPU-light synchronous work can also be used directly as a synchronous durable step.

```python
from async_durable_execution import durable_step, step


def calculate_tax_sync(order: dict) -> dict:
    return {"amount": round(order["subtotal"] * 0.0825, 2)}


@durable_step
def calculate_tax(order: dict) -> dict:
    return calculate_tax_sync(order)


tax = await step(calculate_tax(order), name="calculate-tax")
```

For expensive CPU-bound work, consider moving it to another service or Lambda
function and invoking it durably. Worker threads avoid blocking the event loop,
but they do not make CPU-heavy Python code parallel under the GIL.

## Synchronous Side Effects

Treat synchronous side effects the same way you treat async side effects: put them in
steps and make them idempotent where possible.

Python cannot forcibly stop a worker thread. If a parent `map()` or `parallel()`
operation reaches an early completion condition while a synchronous branch is
running, the SDK cancels the branch task but waits for the started worker function to
settle before the parent completes. Worker functions that are still queued are
cancelled without running. The cancelled child does not checkpoint its return value
or error; the parent `BatchResult` records it as `BatchItemStatus.CANCELLED` for
deterministic replay.

```python
import smtplib
from email.message import EmailMessage

from async_durable_execution import durable_execution, durable_step, step


def send_email_sync(to_address: str, subject: str, body: str) -> None:
    message = EmailMessage()
    message["To"] = to_address
    message["Subject"] = subject
    message.set_content(body)

    with smtplib.SMTP("smtp.example.com", timeout=10) as client:
        client.send_message(message)


@durable_step
def send_email(to_address: str, subject: str, body: str) -> None:
    send_email_sync(to_address, subject, body)


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

`wait_for_callback()` accepts a synchronous submitter and runs it in a worker
thread. Keep the external notification inside that submitter.

```python
from datetime import timedelta
from functools import partial

from async_durable_execution import (
    durable_execution,
    get_wait_for_callback_context,
    wait_for_callback,
)


def submit_approval_sync(callback_id: str, approver_email: str) -> None:
    send_approval_email(approver_email, callback_id)


def submit_approval(approver_email: str) -> None:
    callback_context = get_wait_for_callback_context()
    submit_approval_sync(
        callback_context.callback_id,
        approver_email,
    )


@durable_execution
async def handler(event: dict) -> dict:
    approval = await wait_for_callback(
        partial(submit_approval, event["approver_email"]),
        timeout=timedelta(hours=24),
        name="approval",
    )
    return {"approval": approval}
```

## Avoid Nested Event Loops

Do not call `asyncio.run()` inside a durable handler or durable callable. The
`@durable_execution` wrapper owns the event loop for the invocation.

If you have an async library, await it normally from your async durable callable. If you
have a synchronous library, prefer a synchronous executable callable so the
SDK dispatches the entire body to a worker thread. If an async callable needs
to make an isolated blocking call, use `asyncio.to_thread()` explicitly.

## Runnable Examples

The repository's
[`examples/sync_functions`](https://github.com/zhongkechen/async-durable-execution/tree/main/examples/sync_functions)
package includes complete handlers demonstrating:

- synchronous functions and bound methods used as durable steps
- synchronous map item functions and parallel branches

Each example has a matching local and cloud runner test under
`test_examples/sync_functions`.

## Checklist

1. Define every `@durable_execution` handler with `async def`.
2. Use either `def` or `async def` for other executable user callables.
3. Treat sync callables as leaves that return ordinary values.
4. Use `async def` for every child context, node, item, or branch that
   creates durable operations or awaits async libraries.
5. Keep declarative and configuration hooks synchronous; keep DAG definitions
   and structural or metadata hooks deterministic and side-effect free.
6. Call deterministic synchronous helpers directly only when they do not block.
7. Put synchronous I/O, external reads, and writes inside `@durable_step` functions.
8. Do not perform durable operations from inside a step.
9. Do not call `asyncio.run()` from durable code.
10. Name each important synchronous step so tests and logs stay
   clear.

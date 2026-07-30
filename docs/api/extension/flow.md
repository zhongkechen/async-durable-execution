# Flow and DAG Workflows

Internal implementation module: `async_durable_execution._extension.flow`.

Use a durable directed acyclic graph (DAG) when workflow structure is known
before execution but independent nodes should run concurrently or follow
conditional success and failure routes.

A DAG workflow has three parts:

1. Sync or async node functions decorated with `@durable_node`.
2. A synchronous graph definition decorated with `@durable_dag`.
3. An awaited `flow()` call inside a durable handler, child context, or flow
   node.

## Quick Start

Node bodies can call `step()`, `wait()`, `invoke()`, child contexts, or other
durable operations. The graph definition only declares structure.

```python
from typing import cast

from async_durable_execution import (
    durable_callable,
    durable_dag,
    durable_execution,
    durable_node,
    flow,
    node,
    step,
)


@durable_callable
async def fetch_order(order_id: str) -> dict:
    return {"id": order_id, "status": "ready"}


@durable_callable
async def charge_order(order: dict) -> dict:
    return {"orderId": order["id"], "charged": True}


@durable_node
async def load(order_id: str) -> dict:
    return await step(fetch_order(order_id), name="fetch-order")


@durable_node
async def charge(order: dict) -> dict:
    return await step(charge_order(order), name="charge-order")


@durable_dag
def order_flow(order_id: str) -> dict:
    order = node(load(order_id), name="load")
    payment = node(charge(order.outcome), name="charge")
    return payment.outcome


@durable_execution
async def handler(event: dict) -> dict:
    result = await flow(
        order_flow(str(event["orderId"])),
        name="order-flow",
    )
    return cast(dict, result.output)
```

Passing `order.outcome` to `charge()` creates a success dependency and injects
the successful value when the node starts. The two nodes run in separate
durable child contexts, so their nested operations have isolated checkpoint
histories.

!!! important "A flow node is not an atomic step"

    A node body may replay until its child context completes. Put API calls,
    database access, randomness, and other side effects in `step()` or another
    appropriate durable operation inside the node.

## Definition and Execution Phases

`@durable_dag` must decorate a synchronous function. Calling the decorated
function binds its arguments; `flow()` then evaluates it synchronously and
validates the complete graph before starting any node.

DAG definition code must be deterministic. It must not:

- Call an API, query a database, read the clock, generate randomness, or cause
  another external side effect.
- Start a durable operation such as `step()`, `wait()`, `invoke()`, or another
  `flow()`.
- Depend on mutable state that can change between replays.

Put nondeterministic work and durable operations inside `@durable_node`
functions. A synchronous node runs in a worker thread; use an async node when
its body creates durable operations. Synchronous nodes are leaves and must
return ordinary values. Give every `flow()` and `node()` a stable name. If
`name` is omitted, `node()` uses the decorated function name, which must still
be unique within the graph.

## Inputs and Inferred Dependencies

A node projection used as an argument automatically creates a dependency:

| Projection | Required status | Injected value | Handles failure |
| --- | --- | --- | --- |
| `source.outcome` | `SUCCEEDED` | The successful node value | No |
| `source.error` | `FAILED` | The captured `ErrorObject` | Yes |
| `source.result` | Any terminal status | The complete `FlowNodeResult` | No |

```python
@durable_node
async def run_risky_work() -> str:
    raise RuntimeError("work failed")


@durable_node
async def recover(error):
    return {"recovered": True, "message": error.message}


@durable_dag
def recovery_flow():
    source = node(run_risky_work(), name="source")
    recovery = node(recover(source.error), name="recovery")
    return recovery.result
```

Projections may be nested inside `list`, `tuple`, and `dict` arguments.
Iterators, sets, frozensets, dataclass fields, and other unsupported containers
are rejected when they contain a projection. Materialize an iterator as a list
or tuple before passing it to a node.

Before invoking a node, the SDK resolves projections and clones its complete
argument graph using flow checkpoint serialization. Each node therefore receives
consumer-local `args` and `kwargs`; mutating them does not change definition-time
values or inputs observed by sibling nodes. Bound values must use types supported
by the default checkpoint serializer.

## Conditional Dependencies

Use dependency expressions when a node does not require a projected value as
an argument:

| Expression | Behavior |
| --- | --- |
| `a >> b` | Run `b` after `a` succeeds |
| `a.succeeded >> b` | Run `b` after `a` succeeds |
| `a.failed >> b` | Run `b` after `a` fails and handle that failure |
| `a.completed >> b` | Run `b` after any logical terminal status |
| `(a & b) >> c` | Run `c` after both success conditions match |
| `(a \| b) >> c` | Run `c` after the first success condition matches |
| `a >> (b, c)` | Fan out from `a` to both targets |

`FlowNode` operands use the `succeeded` condition by default. Use parentheses
around `&` and `|` expressions. A target accepts one dependency expression, so
combine conditions before assigning them.

The same expression can be passed directly to `node()`:

```python
decision = node(
    choose_route(),
    name="decision",
    dependency=(payment.succeeded & inventory.succeeded) | review.succeeded,
)
```

Input-derived and explicit dependencies are combined with `AND`. Declaring the
same source as both an input and an explicit dependency is rejected.

## Reading Results for Complex Conditions

For an explicit dependency, use `get_node_context()` to read settled direct
dependency results by stable node name. This keeps the node function independent
of `FlowNode` handles captured by the definition closure.

```python
from async_durable_execution import (
    FlowNodeStatus,
    get_node_context,
)


@durable_node
async def choose_route() -> str:
    context = get_node_context()
    for name in ("payment", "inventory", "review"):
        result = context.get_dependency_result(name)
        if result is not None and result.status is FlowNodeStatus.SUCCEEDED:
            return f"selected:{name}:{result.outcome}"
    raise RuntimeError("No successful route is available")
```

Available helpers are:

- `context.get_dependency_result(name)` returns a settled result or `None`.
- `context.require_dependency_result(name)` returns a settled result or raises
  `InvalidStateError`.
- `context.dependency_results` returns all currently available direct results
  keyed by name.
- `context.result(node)` and `node.result` read a declared direct dependency
  when a handle is intentionally available.

With an `OR` dependency, the target starts after the first matching branch.
Other direct dependencies may not have settled yet, so use
`get_dependency_result()` when reading them.

## Failure Handling

An ordinary exception from a node becomes a logical `FAILED` node result. It
does not immediately stop unrelated branches.

A matching `.failed` dependency handles its source failure. An `.error` input
also creates a failed dependency and handles the failure. `.completed` and
`.result` observe failure without handling it.

After runnable nodes settle, `flow()` raises `FlowExecutionError` if any logical
failure remains unhandled. The exception's `result` attribute contains the
checkpointed `FlowResult`.

```python
from async_durable_execution import FlowExecutionError


try:
    result = await flow(order_flow(order_id), name="order-flow")
except FlowExecutionError as error:
    failed = error.result.unhandled_failures
    raise
```

SDK control failures such as checkpoint, serialization, and invocation errors
are not logical node failures. They propagate out of the DAG and do not
activate `.failed` routes.

## Outputs and Execution Pruning

A DAG definition returns one of:

- `node.outcome`
- `node.error`
- `node.result`
- A tuple of these projections
- `None`

Returning a `FlowNode` directly is invalid. `FlowResult.output` returns `None`,
one projected value, or a tuple according to the definition's output arity.
`FlowResult.outputs` is always a tuple, and `FlowResult.results` contains every
declared node keyed by name.

Execution is derived backward from the selected output nodes. Nodes outside
that reverse-reachable dependency graph do not create child operations and
appear as `SKIPPED`. A definition that returns `None` executes no nodes.

An `.outcome` output requires the selected node to succeed. For a conditional
branch that may be failed or skipped, return `node.result` instead. Returning
`.error` or `.result` as an output explicitly handles a selected failed
output.

## Constraints

- The graph must be acyclic; cycles and self-dependencies raise
  `FlowDefinitionError`.
- Every node has one logical activation. Loops and repeated state-machine
  transitions are not supported.
- Node names must be non-empty and unique.
- All dependencies must belong to the same DAG definition.
- Graph structure and definition-time values must be deterministic across
  replay.

## API

::: async_durable_execution._extension.flow
    options:
      members:
        - FlowDefinitionError
        - FlowExecutionError
        - durable_dag
        - durable_node
        - flow
        - get_node_context
        - node
        - FlowNode
        - FlowNodeContext
        - FlowNodeResult
        - FlowNodeStatus
        - FlowResult

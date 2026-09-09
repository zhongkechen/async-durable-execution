"""Compile public DAG declarations, then schedule their journalled activations.

The scheduler owns readiness. Nodes do not await other node tasks. A persisted
guard decision records the inputs observed when a node becomes eligible, so an
OR choice cannot change on a later invocation.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections import deque
from collections.abc import Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Generic, Iterable, ParamSpec

from ._effects import reserve, scope_effect, step_effect
from ._journal import Pause, cursor
from ._scope import (
    DurableContext,
    binding,
    defining,
    get_current_context,
    get_durable_context,
)
from ._serde import default_codec, register_value
from ._types import (
    DurableExecutionsError,
    ErrorObject,
    ExecutionError,
    InvalidStateError,
    InvocationError,
    OperationSubType,
    SerDesError,
    T,
    ValidationError,
)


class FlowDefinitionError(ValidationError):
    """A DAG declaration has invalid nodes, projections, dependencies, or cycles."""

    pass


class FlowExecutionError(DurableExecutionsError):
    """A completed DAG contains unhandled failures or unavailable outcome outputs.

    Attributes:
        result (FlowResult): Complete checkpointed result, including per-node outcomes.
    """

    def __init__(self, message, result):
        """Attach the completed FlowResult to a description of the graph failure."""
        self.result = result
        super().__init__(message)


class FlowNodeStatus(Enum):
    """Logical node outcome: SUCCEEDED, FAILED, or SKIPPED when the body did not run."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class FlowNodeResult(Generic[T]):
    """Logical result of one declared DAG node.

    Attributes:
        status (FlowNodeStatus): Success, failure, or skipped state.
        outcome (T | None): Successful node value; it may legitimately be None.
        error (ErrorObject | None): Failure details for a failed node.
    """

    status: FlowNodeStatus
    outcome: T | None = None
    error: ErrorObject | None = None

    @classmethod
    def succeeded(cls, outcome):
        """Create a successful node result containing the supplied outcome."""
        return cls(FlowNodeStatus.SUCCEEDED, outcome)

    @classmethod
    def failed(cls, error):
        """Create a failed node result containing an ErrorObject."""
        return cls(FlowNodeStatus.FAILED, error=error)

    @classmethod
    def skipped(cls):
        """Create a result for a node whose body was not executed."""
        return cls(FlowNodeStatus.SKIPPED)

    def to_dict(self):
        """Return status, outcome, and serialized error fields as a mapping."""
        return {
            "status": self.status.value,
            "outcome": self.outcome,
            "error": self.error.to_dict() if self.error else None,
        }

    @classmethod
    def from_dict(cls, data):
        """Restore a node result from its mapping representation."""
        return cls(
            FlowNodeStatus(data["status"]),
            data.get("outcome"),
            ErrorObject.from_dict(data["error"]) if data.get("error") else None,
        )


@dataclass(frozen=True)
class FlowResult:
    """All declared node results and the DAG's selected output projections.

    Nodes outside the output dependency graph are included as SKIPPED. Selecting
    a failed node's error or result as an output handles that failure; an unavailable
    outcome output causes flow() to raise FlowExecutionError with this result.

    Attributes:
        results (dict[str, FlowNodeResult]): Node results keyed by their unique names.
        outputs (tuple): Selected values, preserving definition output order and arity.
        unhandled_failures (tuple[str, ...]): Failed nodes without a matching failure
            handler.
        unavailable_outputs (tuple[str, ...]): Nodes selected as outcome outputs that
            did not succeed.
    """

    results: dict
    outputs: tuple = ()
    unhandled_failures: tuple = ()
    unavailable_outputs: tuple = ()
    _output_kinds: tuple = field(default=(), repr=False)

    @property
    def output(self):
        """Return None, a single value, or a tuple according to selected output arity."""
        if len(self.outputs) == 1:
            return self.outputs[0]
        return self.outputs or None

    @property
    def has_unhandled_failures(self):
        """Return whether the graph has failed nodes without a matching failure handler."""
        return bool(self.unhandled_failures)

    @property
    def has_unavailable_outputs(self):
        """Return whether an outcome output belongs to a failed or skipped node."""
        return bool(self.unavailable_outputs)

    def get_result(self, name):
        """Return the result recorded for a declared node name.

        Raises:
            KeyError: The graph contains no result with that name.
        """
        return self.results[name]

    def to_dict(self):
        """Return node results, outputs, projection kinds, and failure summaries as a
        mapping.
        """
        kinds = self._output_kinds or ("RESULT",) * len(self.outputs)
        return {
            "results": {
                name: result.to_dict() for name, result in self.results.items()
            },
            "outputs": [
                value.to_dict()
                if kind in ("RESULT", "ERROR") and value is not None
                else value
                for value, kind in zip(self.outputs, kinds)
            ],
            "outputProjections": list(kinds),
            "unhandledFailures": list(self.unhandled_failures),
            "unavailableOutputs": list(self.unavailable_outputs),
        }

    @classmethod
    def from_dict(cls, data):
        """Restore a flow result and its projected output values from a mapping.

        Raises:
            SerDesError: Output projection kinds are unknown or their count is
                inconsistent.
        """
        values = data.get("outputs", [])
        kinds = data.get("outputProjections", ["RESULT"] * len(values))
        if len(kinds) != len(values):
            raise SerDesError("Flow output projection count is invalid")
        outputs = []
        for kind, value in zip(kinds, values):
            if kind not in ("RESULT", "ERROR", "OUTCOME"):
                raise SerDesError("Unknown flow output projection")
            outputs.append(
                FlowNodeResult.from_dict(value)
                if kind == "RESULT"
                else ErrorObject.from_dict(value)
                if kind == "ERROR" and value is not None
                else value
            )
        return cls(
            {name: FlowNodeResult.from_dict(v) for name, v in data["results"].items()},
            tuple(outputs),
            tuple(data.get("unhandledFailures", ())),
            tuple(data.get("unavailableOutputs", ())),
            tuple(kinds),
        )


register_value(
    FlowNodeResult, "flow-node-result", FlowNodeResult.to_dict, FlowNodeResult.from_dict
)
register_value(FlowResult, "flow-result", FlowResult.to_dict, FlowResult.from_dict)


@dataclass(frozen=True)
class Projection:
    """Opaque node outcome, error, or result selection used in DAG arguments and outputs."""

    source: Any
    kind: str

    def value(self, result):
        if self.kind == "RESULT":
            return result
        return (
            result.outcome
            if self.kind == "OUTCOME" and result.status is FlowNodeStatus.SUCCEEDED
            else result.error
            if self.kind == "ERROR"
            else None
        )


_build: ContextVar[Any] = ContextVar("ade.graph-definition", default=None)


@dataclass(frozen=True)
class Guard:
    """Opaque dependency expression composed from node success, failure, and completion
    guards.
    """

    plan: Any
    operator: str
    arguments: tuple

    def _combine(self, other, operator):
        self.plan.check()
        other = guard(other)
        if other.plan is not self.plan:
            raise FlowDefinitionError("Cannot combine nodes from different graphs")
        return Guard(self.plan, operator, (self, other))

    def __and__(self, other):
        """Require this expression and another node or expression to match before the
        target runs.
        """
        return self._combine(other, "ALL")

    def __or__(self, other):
        """Allow the target to run after the first matching expression or node success
        condition.
        """
        return self._combine(other, "ANY")

    def __rshift__(self, target):
        """Assign this dependency expression to one node or a tuple of target nodes."""
        targets = target if isinstance(target, tuple) else (target,)
        if not targets:
            raise FlowDefinitionError("A dependency needs at least one target")
        for node in targets:
            self.plan.attach(node, self)
        return target

    def leaves(self):
        if self.operator == "LEAF":
            return (self.arguments,)
        return tuple(leaf for child in self.arguments for leaf in child.leaves())

    def evaluate(self, results):
        if self.operator == "LEAF":
            node, condition = self.arguments
            if node.name not in results:
                return None, set()
            status = results[node.name].status.value
            matched = condition == "COMPLETED" or condition == status
            return matched, {node.name} if matched and condition == "FAILED" else set()
        choices = [child.evaluate(results) for child in self.arguments]
        if self.operator == "ANY":
            for matched, handled in choices:
                if matched is True:
                    return True, handled
            return (None if any(m is None for m, _ in choices) else False), set()
        if any(m is None for m, _ in choices):
            return None, set()
        if not all(m for m, _ in choices):
            return False, set()
        return True, set().union(*(h for _, h in choices))


def guard(value):
    if isinstance(value, FlowNode):
        return value.succeeded
    if isinstance(value, Guard):
        return value
    raise FlowDefinitionError("Dependencies require a node or a dependency expression")


class FlowNode(Generic[T]):
    """Handle for a declared node and its dependency expressions or projections.

    Create handles with node() while a durable_dag definition is evaluating.
    During definition, outcome, error, and result are symbolic inputs or outputs.
    Within a running node, those properties read a declared direct dependency.
    Use get_node_context() for name-based dependency access.

    Attributes:
        name (str): Unique stable node name within its DAG.
    """

    def __init__(self, builder, index, func, name):
        """Initialize a definition-owned handle; application code should call node().

        Args:
            builder (Any): SDK-owned definition context.
            index (int): Declaration position in the graph.
            func (Callable): Bound callable produced by durable_node().
            name (str): Unique nonblank node name.
        """
        self._plan, self._index, self._call, self.name = builder, index, func, name
        self._guard = None

    def _get(self, kind):
        if _build.get() is self._plan:
            return Projection(self, kind)
        result = get_node_context().result(self)
        return Projection(self, kind).value(result)

    @property
    def outcome(self):
        """Project a successful node value during definition, or read that dependency
        value.

        Using this projection as an input requires success. Selecting it as a graph
        output makes an unsuccessful or skipped source an unavailable output.
        """
        return self._get("OUTCOME")

    @property
    def error(self):
        """Project failure details during definition, or read a direct dependency's error.

        An error input requires the source to fail and handles that failure. Successful
        or skipped nodes have no error value.
        """
        return self._get("ERROR")

    @property
    def result(self):
        """Project or read the complete FlowNodeResult, including failed and skipped
        states.

        A result input waits for any terminal status but does not itself handle a
        failure. Selecting result as a graph output explicitly handles that failure.
        """
        return self._get("RESULT")

    @property
    def status(self):
        """Read the status of this declared direct dependency inside a running node."""
        return get_node_context().result(self).status

    @property
    def succeeded(self):
        """Build a definition-time dependency condition requiring this node to succeed."""
        self._plan.check()
        return Guard(self._plan, "LEAF", (self, "SUCCEEDED"))

    @property
    def failed(self):
        """Build a definition-time failure condition that handles its source when matched."""
        self._plan.check()
        return Guard(self._plan, "LEAF", (self, "FAILED"))

    @property
    def completed(self):
        """Build a condition matching any logical terminal status without handling failure."""
        self._plan.check()
        return Guard(self._plan, "LEAF", (self, "COMPLETED"))

    def __and__(self, other):
        """Combine this node's success condition with another dependency using AND."""
        return self.succeeded & other

    def __or__(self, other):
        """Combine this node's success condition with another dependency using OR."""
        return self.succeeded | other

    def __rshift__(self, target):
        """Require this node to succeed before one target node or a tuple of targets runs."""
        return self.succeeded >> target


@dataclass(frozen=True)
class FlowNodeContext(DurableContext):
    """Durable context exposing settled direct dependencies to a running DAG node.

    Access it with get_node_context(). With OR dependencies, some sources may not
    have settled when this node starts. Use get_dependency_result() for optional
    access. Dependency values are cloned for each consumer.
    """

    _direct_dependencies: Any = field(default_factory=frozenset, repr=False)
    _dependency_results: Any = field(default_factory=dict, repr=False)

    def result(self, dependency):
        """Return a settled direct dependency's complete result using its handle.

        Args:
            dependency (FlowNode): A declared direct source of this node.

        Raises:
            InvalidStateError: The handle is not a direct dependency or has not settled.
        """
        if dependency not in self._direct_dependencies:
            raise InvalidStateError("Node is not a direct dependency")
        return self.require_dependency_result(dependency.name)

    @property
    def dependency_results(self):
        """Return a read-only mapping of available direct dependency results keyed by name."""
        return MappingProxyType(self._dependency_results)

    def get_dependency_result(self, name):
        """Read a direct dependency by name, returning None if it is not available yet.

        Raises:
            InvalidStateError: The name is not a declared direct dependency.
        """
        if name not in {node.name for node in self._direct_dependencies}:
            raise InvalidStateError(f"Undeclared dependency: {name}")
        return self._dependency_results.get(name)

    def require_dependency_result(self, name):
        """Read a direct dependency by name and require an available result.

        Raises:
            InvalidStateError: The dependency is undeclared or has not settled.
        """
        value = self.get_dependency_result(name)
        if value is None:
            raise InvalidStateError(f"Dependency has not settled: {name}")
        return value


def get_node_context():
    """Return the active DAG node's durable context and dependency results.

    Raises:
        RuntimeError: Called outside a running flow node.
    """
    context = get_current_context()
    if not isinstance(context, FlowNodeContext):
        raise RuntimeError("get_node_context() requires a running flow node")
    return context


def tokens(value, active=None, supported=True):
    if isinstance(value, Projection):
        if not supported:
            raise FlowDefinitionError("Projection in an unsupported argument container")
        return [value]
    if isinstance(value, Iterator):
        raise FlowDefinitionError(
            "Materialize iterator arguments before defining nodes"
        )
    if isinstance(
        value, (str, bytes, bytearray, memoryview, int, float, bool, type(None))
    ) or callable(value):
        return []
    active = set() if active is None else active
    if id(value) in active:
        raise FlowDefinitionError("Recursive argument containers are unsupported")
    active = active | {id(value)}
    children: Iterable[Any] = []
    if isinstance(value, dict):
        children = [*value.keys(), *value.values()]
    elif isinstance(value, (list, tuple)):
        children = value
    else:
        supported = False
        if isinstance(value, (set, frozenset)):
            children = value
        elif is_dataclass(value) and not isinstance(value, type):
            children = [getattr(value, f.name) for f in fields(value)]
        elif hasattr(value, "__dict__") and not isinstance(value, FlowNode):
            children = vars(value).values()
    return [token for item in children for token in tokens(item, active, supported)]


class Plan:
    def __init__(self):
        self.nodes: list[FlowNode[Any]] = []
        self.outputs: tuple[Projection, ...] = ()
        self.live: list[FlowNode[Any]] = []

    def check(self):
        if _build.get() is not self:
            raise InvalidStateError("Graph handles are valid only in their definition")

    def attach(self, target, condition):
        self.check()
        if (
            not isinstance(target, FlowNode)
            or target._plan is not self
            or condition.plan is not self
        ):
            raise FlowDefinitionError("Dependency and target must belong to this graph")
        if target._guard is not None:
            raise FlowDefinitionError("A node may have only one dependency expression")
        target._guard = condition

    def compile(self, output):
        self.outputs = (
            () if output is None else output if isinstance(output, tuple) else (output,)
        )
        if any(
            not isinstance(p, Projection) or p.source._plan is not self
            for p in self.outputs
        ):
            raise FlowDefinitionError("DAG outputs must be node projections")
        names: set[str] = set()
        incoming = {}
        outgoing: dict[FlowNode[Any], list[FlowNode[Any]]] = {
            node: [] for node in self.nodes
        }
        for node in self.nodes:
            if (
                not isinstance(node.name, str)
                or not node.name.strip()
                or node.name in names
            ):
                raise FlowDefinitionError("Node names must be unique nonblank strings")
            names.add(node.name)
            deps = [source for source, _ in node._guard.leaves()] if node._guard else []
            if len(set(deps)) != len(deps):
                raise FlowDefinitionError("Duplicate dependency in a node expression")
            if any(source not in outgoing or source is node for source in deps):
                raise FlowDefinitionError("Invalid or self dependency")
            incoming[node] = len(deps)
            for source in deps:
                outgoing[source].append(node)
        ready = deque(node for node in self.nodes if incoming[node] == 0)
        ordered = []
        while ready:
            node = ready.popleft()
            ordered.append(node)
            for target in outgoing[node]:
                incoming[target] -= 1
                if incoming[target] == 0:
                    ready.append(target)
        if len(ordered) != len(self.nodes):
            raise FlowDefinitionError("DAG contains a dependency cycle")
        needed = {p.source for p in self.outputs}
        todo = list(needed)
        while todo:
            current = todo.pop()
            for source, _ in current._guard.leaves() if current._guard else ():
                if source not in needed:
                    needed.add(source)
                    todo.append(source)
        self.live = [node for node in ordered if node in needed]


def durable_node(func):
    """Bind arguments to an async function used as a DAG node body.

    Calling the decorated function validates argument binding and produces a
    zero-argument callable for node(). The body runs after graph validation and
    may compose durable operations; external side effects belong in steps.

    Args:
        func (Callable): Async node function.

    Returns:
        (Callable): A wrapper producing a bound node callable without executing it.

    Raises:
        FlowDefinitionError: The decorated function is not asynchronous.
    """
    if isinstance(func, (classmethod, staticmethod)):
        return type(func)(durable_node(func.__func__))
    if not inspect.iscoroutinefunction(func):
        raise FlowDefinitionError("@durable_node requires an async function")

    @functools.wraps(func)
    def bind(*args, **kwargs):
        inspect.signature(func).bind(*args, **kwargs)
        call = functools.partial(func, *args, **kwargs)
        setattr(call, "__name__", func.__name__)
        setattr(call, "_ade_node", (func, args, kwargs))
        return call

    return bind


def durable_dag(func):
    """Bind arguments to a synchronous DAG definition without evaluating it.

    Pass the resulting callable to flow(), which evaluates and validates the
    complete declaration before scheduling durable work.

    Args:
        func (Callable): Synchronous function declaring nodes and selecting projections.

    Returns:
        (Callable): A wrapper producing a deferred graph definition.

    Raises:
        FlowDefinitionError: The decorated function is asynchronous.
    """
    if isinstance(func, (classmethod, staticmethod)):
        return type(func)(durable_dag(func.__func__))
    if inspect.iscoroutinefunction(func):
        raise FlowDefinitionError("@durable_dag requires a synchronous definition")

    @functools.wraps(func)
    def bind(*args, **kwargs):
        call = functools.partial(func, *args, **kwargs)
        setattr(call, "__name__", func.__name__)
        setattr(call, "_ade_dag", True)
        return call

    return bind


def node(
    func: Callable[[], Awaitable[T]],
    *,
    name: str | None = None,
    dependency: FlowNode[Any] | Guard | None = None,
) -> FlowNode[T]:
    """Declare a bound durable_node callable in the active DAG definition.

    Projected arguments infer dependencies: outcome requires success, error
    requires failure and handles it, and result accepts any terminal state.
    Projections may be nested in lists, tuples, or dictionaries. Explicit
    conditions combine with inferred dependencies using AND.

    Args:
        func: Bound callable produced by durable_node().
        name: Unique stable name; defaults to the node function name.
        dependency: Optional node or expression composed using &, |, and status guards.

    Returns:
        (FlowNode[T]): A handle for dependency construction and input/output projections.

    Raises:
        InvalidStateError: No DAG definition is active.
        FlowDefinitionError: The callable or its projected inputs are invalid.
    """
    plan = _build.get()
    if plan is None:
        raise InvalidStateError("node() requires an active DAG definition")
    if not hasattr(func, "_ade_node"):
        raise FlowDefinitionError("node() requires a bound @durable_node call")
    references = tokens((func._ade_node[1], func._ade_node[2]))
    requirements: dict[FlowNode[Any], str] = {}
    for projection in references:
        if projection.source._plan is not plan:
            raise FlowDefinitionError("Projected arguments must belong to this graph")
        previous = requirements.setdefault(projection.source, projection.kind)
        if previous != projection.kind:
            raise FlowDefinitionError("Conflicting projections of the same dependency")
    conditions = [
        Guard(
            plan,
            "LEAF",
            (
                source,
                {"OUTCOME": "SUCCEEDED", "ERROR": "FAILED", "RESULT": "COMPLETED"}[
                    kind
                ],
            ),
        )
        for source, kind in requirements.items()
    ]
    if dependency is not None:
        conditions.append(guard(dependency))
    result: FlowNode[Any] = FlowNode(plan, len(plan.nodes), func, name or func.__name__)
    plan.nodes.append(result)
    if conditions:
        condition = conditions[0]
        for extra in conditions[1:]:
            condition = condition & extra
        plan.attach(result, condition)
    return result


def resolve(value, results):
    if isinstance(value, Projection):
        return value.value(results[value.source.name])
    if isinstance(value, dict):
        return {
            resolve(key, results): resolve(item, results) for key, item in value.items()
        }
    if isinstance(value, list):
        return [resolve(item, results) for item in value]
    if isinstance(value, tuple):
        items = [resolve(item, results) for item in value]
        return type(value)(*items) if hasattr(value, "_fields") else tuple(items)
    return value


def clone(value):
    return default_codec.deserialize_sync(default_codec.serialize_sync(value))


def control_error(error):
    todo, visited = [error], set()
    while todo:
        item = todo.pop()
        if id(item) in visited:
            continue
        visited.add(id(item))
        if isinstance(item, (InvocationError, ExecutionError, SerDesError)):
            return item
        todo.extend(getattr(item, "exceptions", ()))
        cause = item.__cause__ or item.__context__
        if cause is not None:
            todo.append(cause)
    return None


async def execute_plan(plan):
    namespace = cursor()
    node_slots = {
        node: namespace.reserve(node.name, f"node/{node.name}") for node in plan.live
    }
    guard_slots = {
        node: namespace.reserve(None, f"guard/{node.name}") for node in plan.live
    }
    decisions = {}
    results: dict[str, FlowNodeResult[Any]] = {}
    handled: set[str] = set()
    paused: list[Pause] = []
    active, launched = {}, set()
    try:
        while len(results) < len(plan.live):
            for node in plan.live:
                if node in launched:
                    continue
                dependencies = (
                    {source for source, _ in node._guard.leaves()}
                    if node._guard
                    else set()
                )
                guard_slot = guard_slots[node]
                if guard_slot.entry and guard_slot.entry.status == "SUCCEEDED":
                    decision = clone(
                        default_codec.deserialize_sync(guard_slot.entry.payload)
                    )
                    if any(name not in results for name in decision["selected"]):
                        continue
                else:
                    matched, observed = (
                        node._guard.evaluate(results) if node._guard else (True, set())
                    )
                    if matched is None:
                        continue
                    decision = {
                        "matched": matched,
                        "selected": [
                            source.name
                            for source in sorted(dependencies, key=lambda n: n._index)
                            if source.name in results
                        ],
                        "handled": sorted(observed),
                    }
                guard_slot.select("STEP", "ade3-flow-guard")

                async def choose(value=decision):
                    return value

                decision = await guard_slot.spawn(step_effect(guard_slot, choose))
                decisions[node] = decision
                handled.update(decision["handled"])
                launched.add(node)
                ticket = node_slots[node].select(
                    "CONTEXT", OperationSubType.RUN_IN_CHILD_CONTEXT
                )
                inputs = clone({name: results[name] for name in decision["selected"]})
                view = ticket.child(
                    kind=FlowNodeContext,
                    _direct_dependencies=frozenset(dependencies),
                    _dependency_results=inputs,
                )

                async def activate(current=node, choice=decision, inputs=inputs):
                    if not choice["matched"]:
                        return FlowNodeResult.skipped()
                    function, args, kwargs = current._call._ade_node
                    arguments, keywords = clone(
                        (resolve(args, inputs), resolve(kwargs, inputs))
                    )
                    try:
                        value = await function(*arguments, **keywords)
                    except Exception as error:
                        signal = control_error(error)
                        if signal is not None:
                            raise signal
                        return FlowNodeResult.failed(ErrorObject.from_exception(error))
                    return FlowNodeResult.succeeded(value)

                task = ticket.spawn(scope_effect(ticket, activate, context=view))
                active[task] = node
            if not active:
                if paused:
                    times = [p.until for p in paused if p.until is not None]
                    raise Pause(min(times) if times else None)
                if len(results) < len(plan.live):
                    raise InvalidStateError("DAG cannot make progress")
                break
            completed, _ = await asyncio.wait(
                active, return_when=asyncio.FIRST_COMPLETED
            )
            for task in sorted(completed, key=lambda task: active[task]._index):
                current = active.pop(task)
                try:
                    results[current.name] = task.result()
                except Pause as pause:
                    paused.append(pause)
    finally:
        for task in active:
            if not task.done():
                task.cancel()
        await asyncio.gather(*active, return_exceptions=True)
    all_results = {
        node.name: results.get(node.name, FlowNodeResult.skipped())
        for node in plan.nodes
    }
    handled.update(p.source.name for p in plan.outputs if p.kind in ("RESULT", "ERROR"))
    return FlowResult(
        all_results,
        tuple(p.value(all_results[p.source.name]) for p in plan.outputs),
        tuple(
            name
            for name, value in all_results.items()
            if value.status is FlowNodeStatus.FAILED and name not in handled
        ),
        tuple(
            dict.fromkeys(
                p.source.name
                for p in plan.outputs
                if p.kind == "OUTCOME"
                and all_results[p.source.name].status is not FlowNodeStatus.SUCCEEDED
            )
        ),
        tuple(p.kind for p in plan.outputs),
    )


def flow(
    definition: Callable[[], Any], *, name: str | None = None
) -> asyncio.Task[FlowResult]:
    """Validate a DAG definition and start its durable execution task.

    Validation occurs synchronously before durable work starts. Only nodes reachable
    backward from selected outputs execute; other declarations appear as SKIPPED.
    Definitions return outcome, error, or result projections, a tuple of projections,
    or None. Dependency choices persist across replay.

    Args:
        definition: Bound synchronous definition produced by durable_dag().
        name: Stable flow scope name; defaults to the definition name.

    Returns:
        (asyncio.Task[FlowResult]): Per-node results and selected output values.

    Raises:
        FlowDefinitionError: Nodes, outputs, names, or dependencies are invalid or
            cyclic.
        FlowExecutionError: Completed execution has unhandled failures or unavailable
            outputs.
        RuntimeError: No durable handler or child scope is active.
    """
    get_durable_context()
    if not getattr(definition, "_ade_dag", False):
        raise FlowDefinitionError("flow() requires a bound @durable_dag definition")
    plan = Plan()
    build_token, operation_token = _build.set(plan), defining.set(True)
    try:
        output = definition()
        if inspect.isawaitable(output):
            if inspect.iscoroutine(output):
                output.close()
            raise FlowDefinitionError("DAG definitions must be synchronous")
        plan.compile(output)
    finally:
        _build.reset(build_token)
        defining.reset(operation_token)
    ticket = reserve(
        "CONTEXT",
        OperationSubType.RUN_IN_CHILD_CONTEXT,
        name or getattr(definition, "__name__", "flow"),
    )

    async def run():
        result = await scope_effect(ticket, functools.partial(execute_plan, plan))
        if result.has_unhandled_failures or result.has_unavailable_outputs:
            raise FlowExecutionError(
                "Flow has unhandled failures or unavailable outputs", result
            )
        return result

    return ticket.spawn(run())

"""An in-memory journal backend and deterministic local invocation driver."""

from __future__ import annotations

import asyncio
import copy
import json
import time
import uuid
from types import SimpleNamespace
from typing import Any

from ._effects import error_record, invoke_workflow
from ._journal import Entry
from ._types import ErrorObject, InvocationError, InvalidStateError
from ._views import DurableFunctionTestResult


class MemoryBackend:
    def __init__(self, arn, value, name, mocks):
        self.arn, self.now, self.sequence = arn, time.time(), 0
        root = Entry(
            arn.rsplit("/", 1)[-1],
            "EXECUTION",
            "STARTED",
            name=name,
            payload=json.dumps(value),
            started=self.now,
        )
        self.entries = {root.key: root}
        self.root = root.key
        self._wall = time.monotonic()
        self.changed = asyncio.Event()
        self.mocks = mocks
        self.outcome = None
        self.updates = []

    @property
    def token(self):
        return str(self.sequence)

    async def transact(self, token, updates):
        if token != self.token:
            raise InvocationError("Checkpoint lease is stale")
        working = dict(self.entries)
        outcome = self.outcome
        for update in updates:
            key, kind, action = update["Id"], update["Type"], update["Action"]
            parent = update.get("ParentId")
            if parent and (
                parent not in working
                or working[parent].kind != "CONTEXT"
                or working[parent].status != "STARTED"
            ):
                raise InvalidStateError("Checkpoint parent is missing or has completed")
            old = copy.deepcopy(working[key]) if key in working else None
            if action == "START":
                if old and old.status != "READY":
                    raise InvalidStateError(
                        "An existing operation cannot be started again"
                    )
                entry = old or Entry(
                    key,
                    kind,
                    "STARTED",
                    update.get("Name"),
                    parent,
                    update.get("SubType"),
                    started=self.now,
                )
                entry.status, entry.due = "STARTED", None
                entry.options = copy.deepcopy(
                    update.get(
                        {
                            "WAIT": "WaitOptions",
                            "CALLBACK": "CallbackOptions",
                            "CHAINED_INVOKE": "ChainedInvokeOptions",
                        }.get(kind, ""),
                        {},
                    )
                )
                if kind == "WAIT":
                    entry.due = self.now + entry.options["WaitSeconds"]
                if kind == "CALLBACK":
                    entry.callback = str(uuid.uuid4())
                    limits = [
                        n
                        for n in (
                            entry.options.get("TimeoutSeconds", 0),
                            entry.options.get("HeartbeatTimeoutSeconds", 0),
                        )
                        if n
                    ]
                    entry.due = self.now + min(limits) if limits else None
                    entry.options["absoluteDeadline"] = (
                        self.now + entry.options.get("TimeoutSeconds", 0)
                        if entry.options.get("TimeoutSeconds")
                        else None
                    )
                if kind == "CHAINED_INVOKE":
                    target = entry.options["FunctionName"]
                    entry.payload = update.get("Payload")
                    if target in self.mocks:
                        entry.payload = json.dumps(self.mocks[target])
                        entry.status, entry.ended = "SUCCEEDED", self.now
                working[key] = entry
            else:
                if old is None or old.status not in ("STARTED", "READY"):
                    raise InvalidStateError(
                        "Operation cannot transition from its current state"
                    )
                entry = old
                entry.status = {
                    "SUCCEED": "SUCCEEDED",
                    "FAIL": "FAILED",
                    "CANCEL": "CANCELLED",
                    "RETRY": "PENDING",
                }[action]
                if "Payload" in update:
                    entry.payload = update["Payload"]
                entry.error = update.get("Error")
                if kind == "STEP":
                    entry.attempt += 1
                if action == "RETRY":
                    entry.due = (
                        self.now + update["StepOptions"]["NextAttemptDelaySeconds"]
                    )
                else:
                    entry.ended = self.now
                entry.replay_children = update.get("ContextOptions", {}).get(
                    "ReplayChildren", False
                )
                if kind == "EXECUTION":
                    outcome = {
                        "Status": entry.status,
                        "Result": entry.payload,
                        "Error": entry.error,
                    }
            working[key] = entry
        self.entries = working
        self.outcome = outcome
        self.sequence += 1
        self.updates.extend(copy.deepcopy(updates))
        self.changed.set()
        return {
            "CheckpointToken": self.token,
            "NewExecutionState": {
                # Callback delivery may update another entry while the handler
                # is active. Publish that state with the checkpoint acknowledgement.
                "Operations": [entry.wire() for entry in working.values()]
            },
        }

    async def page(self, token, marker):
        return {"Operations": [entry.wire() for entry in self.entries.values()]}

    def advance(self, instant):
        self.now = max(self.now, instant)
        self._wall = time.monotonic()
        changed = False
        for entry in self.entries.values():
            if entry.due is None or entry.due > self.now:
                continue
            if entry.kind == "STEP" and entry.status == "PENDING":
                entry.status, entry.due = "READY", None
                changed = True
            elif entry.kind == "WAIT" and entry.status == "STARTED":
                entry.status, entry.ended = "SUCCEEDED", self.now
                changed = True
            elif entry.kind == "CALLBACK" and entry.status == "STARTED":
                entry.status, entry.ended = "TIMED_OUT", self.now
                absolute = entry.options.get("absoluteDeadline")
                heartbeat = entry.options.get("HeartbeatTimeoutSeconds") and (
                    absolute is None or entry.due < absolute
                )
                entry.error = ErrorObject(
                    "Callback heartbeat timed out"
                    if heartbeat
                    else "Callback timed out",
                    "Callback.Heartbeat" if heartbeat else "Callback.Timeout",
                ).to_dict()
                changed = True
        return changed

    def sync_time(self):
        self.advance(self.now + time.monotonic() - self._wall)

    def callback(self, callback_id):
        self.sync_time()
        if self.outcome is not None:
            raise ValueError("Execution has completed")
        entry = next(
            (entry for entry in self.entries.values() if entry.callback == callback_id),
            None,
        )
        if entry is None or entry.status != "STARTED":
            raise ValueError("Callback does not exist or has completed")
        return entry


class DurableFunctionLocalTestRunner:
    """Execute a handler against a fresh local journal, with virtual durable waits."""

    def __init__(
        self,
        handler,
        poll_interval=1.0,
        input=None,
        timeout=900,
        function_name="test-function",
        execution_name="execution-name",
        account_id="123456789012",
    ):
        """Configure a local runner without starting an execution.

        Args:
            handler (Callable): Handler decorated with durable_execution().
            poll_interval (float): Requested callback polling interval in seconds.
            input (Any): Application input; valid JSON strings are decoded before
                execution.
            timeout (int): Local execution time budget in seconds; default 900.
            function_name (str): Function name exposed by the simulated Lambda context.
            execution_name (str): Execution name used in local metadata.
            account_id (str): Account component used in local execution ARNs.
        """
        self.mode = "local"
        self.handler, self.poll_interval, self.input, self.timeout = (
            handler,
            poll_interval,
            input,
            timeout,
        )
        self.function_name, self.execution_name, self.account_id = (
            function_name,
            execution_name,
            account_id,
        )
        self._mocks = {}
        self._backend: MemoryBackend | None = None
        self._task: asyncio.Task | None = None
        self._idle = False

    def __enter__(self):
        """Return this runner for synchronous context-manager use."""
        return self

    def __exit__(self, *args):
        """Request cancellation of active work when leaving a synchronous context."""
        self.close()

    async def __aenter__(self):
        """Return this runner; execution starts when run() or run_async() is called."""
        return self

    async def __aexit__(self, *args):
        """Cancel active work and await its shutdown when leaving the async context."""
        self.close()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

    def close(self):
        """Request cancellation of an active execution.

        Use the async context-manager form to await shutdown and cleanup of its tasks.
        """
        if self._task and not self._task.done():
            self._task.cancel()

    def mock_invoke_result(self, function_name, result):
        """Register a JSON-serializable result for a local chained invocation.

        Args:
            function_name (str): Exact target name or ARN used by invoke() or recurse().
            result (Any): Result returned instead of calling a deployed function.
        """
        self._mocks[function_name] = result

    async def run_async(self):
        """Start a fresh local execution and return its ARN before completion.

        Use the ARN with wait_for_callback() and wait_for_result(). One runner supports
        one active execution at a time.

        Returns:
            (str): Identifier of the new local durable execution.

        Raises:
            RuntimeError: This runner already has active work.
        """
        if self._task and not self._task.done():
            raise RuntimeError("The runner already has an active execution")
        run_id = uuid.uuid4().hex
        self._arn = f"arn:aws:lambda:us-west-2:{self.account_id}:function:{self.function_name}:$LATEST/durable-execution/{self.execution_name}/{run_id}"
        value = self.input
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                pass
        self._backend = MemoryBackend(
            self._arn, value, self.execution_name, self._mocks
        )
        self._task = asyncio.create_task(self._drive())
        self._task.add_done_callback(
            lambda task: None if task.cancelled() else task.exception()
        )
        return self._arn

    def _store(self) -> MemoryBackend:
        if self._backend is None:
            raise RuntimeError("No local execution has started")
        return self._backend

    async def _drive(self):
        backend = self._store()
        started = time.monotonic()
        context = SimpleNamespace(
            aws_request_id="local-" + uuid.uuid4().hex,
            log_group_name=None,
            log_stream_name=None,
            memory_limit_in_mb="128",
            client_context=None,
            identity=None,
            function_name=self.function_name,
            function_version="$LATEST",
            invoked_function_arn=f"arn:aws:lambda:us-west-2:{self.account_id}:function:{self.function_name}:$LATEST",
            tenant_id=None,
            get_remaining_time_in_millis=lambda: max(
                0, int((self.timeout - (time.monotonic() - started)) * 1000)
            ),
            log=lambda msg: print(msg),
        )
        workflow = getattr(self.handler, "_ade_workflow", None)
        if workflow is None:
            raise TypeError("Local runners require a @durable_execution handler")
        failures = 0
        while True:
            if time.monotonic() - started >= self.timeout:
                backend.outcome = {
                    "Status": "FAILED",
                    "Error": ErrorObject(
                        "Execution timed out", "TimeoutError"
                    ).to_dict(),
                }
                break
            self._idle = False
            event = {
                "DurableExecutionArn": self._arn,
                "CheckpointToken": backend.token,
                "InitialExecutionState": {
                    "Operations": [entry.wire() for entry in backend.entries.values()]
                },
            }
            try:
                output = await invoke_workflow(workflow, event, context, backend)
                failures = 0
            except InvocationError as error:
                failures += 1
                if failures > 5:
                    backend.outcome = {"Status": "FAILED", "Error": error_record(error)}
                    break
                await asyncio.sleep(0)
                continue
            self._idle = True
            if backend.outcome is not None:
                break
            if output["Status"] != "PENDING":
                backend.outcome = output
                break
            timed = [
                entry.due
                for entry in backend.entries.values()
                if entry.due is not None
                and (
                    (entry.kind == "WAIT" and entry.status == "STARTED")
                    or (entry.kind == "STEP" and entry.status == "PENDING")
                )
            ]
            if timed:
                backend.advance(min(timed))
                await asyncio.sleep(0)
                continue
            callbacks = [
                entry
                for entry in backend.entries.values()
                if entry.kind == "CALLBACK" and entry.status == "STARTED"
            ]
            if not callbacks and not any(
                entry.kind == "CHAINED_INVOKE" and entry.status == "STARTED"
                for entry in backend.entries.values()
            ):
                backend.outcome = {
                    "Status": "FAILED",
                    "Error": ErrorObject(
                        "Workflow suspended without pending durable work",
                        "InvalidStateError",
                    ).to_dict(),
                }
                break
            backend.changed.clear()
            remaining = self.timeout - (time.monotonic() - started)
            deadlines = [
                entry.due - backend.now for entry in callbacks if entry.due is not None
            ]
            delay = max(0, min([remaining, *deadlines]))
            wall = time.monotonic()
            try:
                await asyncio.wait_for(backend.changed.wait(), delay)
            except asyncio.TimeoutError:
                pass
            backend.sync_time()
        root = backend.entries[backend.root]
        root.status = backend.outcome["Status"]
        root.ended = backend.now
        return DurableFunctionTestResult.create(backend)

    async def run(self):
        """Start an execution and await its final result using configured defaults.

        Returns:
            (DurableFunctionTestResult): Status, result, error, and operation inspection
                views.

        Raises:
            TimeoutError: Waiting for the execution exceeds the runner's wait budget.
        """
        arn = await self.run_async()
        return await self.wait_for_result(arn, self.timeout + 1)

    def _require_execution(self, execution_arn):
        if self._backend is None or execution_arn != self._arn:
            raise ValueError("Unknown local execution")

    async def wait_for_result(self, execution_arn, timeout=60):
        """Wait for an already-started local execution without cancelling it on wait
        timeout.

        Args:
            execution_arn (str): ARN returned by this runner's run_async().
            timeout (float): Maximum time to wait for a result, in seconds.

        Returns:
            (DurableFunctionTestResult): The completed execution's inspection result.

        Raises:
            ValueError: The ARN is not this runner's active execution.
            TimeoutError: No result is available before the wait deadline.
        """
        self._require_execution(execution_arn)
        assert self._task is not None
        return await asyncio.wait_for(asyncio.shield(self._task), timeout)

    async def wait_for_callback(self, execution_arn, name=None, timeout=60):
        """Wait until an active callback is visible at an invocation boundary.

        Args:
            execution_arn (str): ARN returned by this runner.
            name (str | None): Optional callback operation name; None selects an active
                callback.
            timeout (float): Maximum callback discovery wait in seconds.

        Returns:
            (str): Identifier to use with the send_callback_* methods.

        Raises:
            ValueError: Execution is unknown, the named callback has completed, or the
                execution finishes without a matching callback.
            TimeoutError: No matching active callback appears before the deadline.
        """
        self._require_execution(execution_arn)

        async def find():
            while True:
                if self._idle:
                    matches = [
                        entry
                        for entry in self._store().entries.values()
                        if entry.kind == "CALLBACK"
                        and (name is None or entry.name == name)
                    ]
                    pending = [entry for entry in matches if entry.status == "STARTED"]
                    if pending:
                        return pending[-1].callback
                    if name and matches:
                        raise ValueError(f"Callback {name} has already completed")
                assert self._task is not None
                if self._task.done():
                    raise ValueError(
                        "Execution finished without the requested callback"
                    )
                await asyncio.sleep(min(self.poll_interval, 0.01))

        return await asyncio.wait_for(find(), timeout)

    async def send_callback_success(self, callback_id, result=None):
        """Deliver a successful result to an active local callback and wake its execution.

        Args:
            callback_id (str): Active callback identifier.
            result (bytes | None): UTF-8 payload. None and empty bytes complete
                without a value, matching AWS callback payload normalization.

        Raises:
            ValueError: The callback or its execution has already completed or is
                unknown.
        """
        entry = self._store().callback(callback_id)
        entry.status, entry.payload, entry.ended = (
            "SUCCEEDED",
            result.decode() if result else None,
            self._store().now,
        )
        self._store().changed.set()

    async def send_callback_failure(self, callback_id, error=None):
        """Fail an active local callback and wake its execution.

        Args:
            callback_id (str): Active callback identifier.
            error (ErrorObject | None): Failure details; None supplies a generic
                callback failure.

        Raises:
            ValueError: The callback or its execution is unknown or has completed.
        """
        entry = self._store().callback(callback_id)
        entry.status, entry.error, entry.ended = (
            "FAILED",
            (error or ErrorObject("Callback failed")).to_dict(),
            self._store().now,
        )
        self._store().changed.set()

    async def send_callback_heartbeat(self, callback_id):
        """Refresh an active callback's heartbeat deadline without extending its overall
        timeout.

        Args:
            callback_id (str): Callback that is still awaiting a result.

        Raises:
            ValueError: The callback is unknown, expired, or belongs to a completed
                execution.
        """
        entry = self._store().callback(callback_id)
        period = entry.options.get("HeartbeatTimeoutSeconds", 0)
        if period:
            entry.due = self._store().now + period
            absolute = entry.options.get("absoluteDeadline")
            if absolute is not None:
                entry.due = min(entry.due, absolute)
        self._store().changed.set()


def create_local_runner(
    *, handler: Any, poll_interval: float = 1.0, input: Any = None, timeout: int = 60
) -> DurableFunctionLocalTestRunner:
    """Create an in-memory runner for a durable handler without AWS credentials.

    Prefer `async with create_local_runner(...) as runner` so shutdown is awaited.
    Durable waits and retries advance virtual time; callbacks remain available for
    explicit success, failure, and heartbeat delivery.

    Args:
        handler: Handler produced by durable_execution().
        poll_interval: Requested callback discovery interval in seconds.
        input: Application input, or a JSON string to decode before execution.
        timeout: Local execution time budget in seconds; defaults to 60.

    Returns:
        (DurableFunctionLocalTestRunner): A runner with no execution started yet.
    """
    return DurableFunctionLocalTestRunner(handler, poll_interval, input, timeout)

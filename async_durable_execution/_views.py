"""Read-only projections of journal records for the public testing API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from ._journal import Entry, epoch, mapping
from ._serde import default_codec
from ._types import (
    ErrorObject,
    InvocationStatus,
    OperationStatus,
    OperationSubType,
    OperationType,
)


class OperationView:
    """Inspection view returned by a runner's operation lookup methods.

    Attributes:
        operation_id (str): Durable operation identifier.
        operation_type (OperationType): Backend operation category.
        status (OperationStatus): Latest observed operation state.
        name (str | None): Configured operation name.
        parent_id (str | None): Enclosing operation identifier.
        sub_type (OperationSubType | str | None): SDK or extension subtype label.
        start_timestamp (datetime | None): Start time when available.
        end_timestamp (datetime | None): Terminal time when available.
        step_details (Any): Step result, error, and attempt metadata, or None.
        wait_details (Any): Wait deadline metadata, or None.
        callback_details (Any): Callback ID and result/error metadata, or None.
        context_details (Any): Child result/error metadata, or None.
        chained_invoke_details (Any): Invocation result/error metadata, or None.
        execution_details (Any): Root execution input metadata, or None.
    """

    def __init__(self, entry):
        self._entry = entry
        self.operation_id = entry.key
        self.operation_type = OperationType(entry.kind)
        self.status = OperationStatus(entry.status)
        self.name, self.parent_id = entry.name, entry.parent
        try:
            self.sub_type = OperationSubType(entry.subtype)
        except ValueError:
            self.sub_type = entry.subtype
        self.start_timestamp = (
            datetime.fromtimestamp(entry.started, timezone.utc)
            if entry.started is not None
            else None
        )
        self.end_timestamp = (
            datetime.fromtimestamp(entry.ended, timezone.utc)
            if entry.ended is not None
            else None
        )
        details = SimpleNamespace(
            result=entry.payload,
            error=ErrorObject.from_dict(entry.error) if entry.error else None,
            attempt=entry.attempt,
            callback_id=entry.callback,
            next_attempt_timestamp=datetime.fromtimestamp(entry.due, timezone.utc)
            if entry.due is not None
            else None,
            scheduled_end_timestamp=datetime.fromtimestamp(entry.due, timezone.utc)
            if entry.due is not None
            else None,
            input_payload=entry.payload,
            replay_children=entry.replay_children,
        )
        for kind, name in {
            "STEP": "step_details",
            "WAIT": "wait_details",
            "CALLBACK": "callback_details",
            "CONTEXT": "context_details",
            "CHAINED_INVOKE": "chained_invoke_details",
            "EXECUTION": "execution_details",
        }.items():
            setattr(self, name, details if kind == entry.kind else None)

    def to_dict(self):
        """Return the operation record using AWS history field names."""
        return self._entry.wire()


def deserialize(payload, serdes=None):
    return (
        None
        if payload is None or payload == ""
        else (serdes or default_codec).deserialize_sync(payload)
    )


@dataclass(frozen=True)
class DurableFunctionTestResult:
    """Public inspection result shared by local and cloud runners.

    Use named lookup methods for top-level operations, get_child_operations() for
    nested scopes, and get_all_operations() for the combined visible operation list.

    Attributes:
        status (InvocationStatus): Execution outcome represented by the runner.
        operations (list): Visible top-level non-execution operations.
        result (str | None): Serialized execution result, when available.
        error (ErrorObject | None): Execution failure details, when available.
    """

    status: InvocationStatus
    operations: list
    result: str | None = None
    error: ErrorObject | None = None
    _all_operations: list = field(default_factory=list, repr=False, compare=False)

    @classmethod
    def create(cls, execution):
        """Construct inspection views from a completed local execution.

        Args:
            execution (Any): Runner-owned execution source containing operations and a
                final result.

        Returns:
            (DurableFunctionTestResult): Visible operation views and the final outcome.
        """
        if hasattr(execution, "entries"):
            all_ops = [
                OperationView(entry)
                for entry in execution.entries.values()
                if not (entry.subtype or "").startswith("ade3-")
            ]
            outcome = execution.outcome
            return cls(
                InvocationStatus(outcome["Status"]),
                [
                    op
                    for op in all_ops
                    if op.operation_type is not OperationType.EXECUTION
                    and op.parent_id is None
                ],
                outcome.get("Result"),
                ErrorObject.from_dict(outcome["Error"])
                if outcome.get("Error")
                else None,
                all_ops,
            )
        value = execution.result
        all_ops = list(execution.operations)
        return cls(
            value.status,
            [
                op
                for op in all_ops
                if op.operation_type is not OperationType.EXECUTION
                and op.parent_id is None
            ],
            value.result,
            value.error,
            all_ops,
        )

    @classmethod
    def from_execution_history(cls, execution_response, history_response):
        """Combine an AWS execution response with its collected history pages.

        Args:
            execution_response (Any): AWS-style execution mapping or object exposing
                to_dict().
            history_response (Any): AWS-style mapping containing the combined Events
                list.

        Returns:
            (DurableFunctionTestResult): Final outcome and reconstructed operation views.
        """
        execution, history = mapping(execution_response), mapping(history_response)
        entries = history_entries(history.get("Events", []))
        all_ops = [
            OperationView(entry)
            for entry in entries.values()
            if not (entry.subtype or "").startswith("ade3-")
        ]
        status = execution.get("Status")
        if status not in {item.value for item in InvocationStatus}:
            status = "FAILED"
        return cls(
            InvocationStatus(status),
            [
                op
                for op in all_ops
                if op.operation_type is not OperationType.EXECUTION
                and op.parent_id is None
            ],
            execution.get("Result"),
            ErrorObject.from_dict(execution["Error"])
            if execution.get("Error")
            else None,
            all_ops,
        )

    def get_operation_by_name(self, name):
        """Return the first visible top-level operation with the supplied name.

        Raises:
            ValueError: No top-level operation has that name.
        """
        for item in self.operations:
            if item.name == name:
                return item
        raise ValueError(f"Operation not found: {name}")

    def _find(self, name, kind):
        value = self.get_operation_by_name(name)
        if value.operation_type is not kind:
            raise ValueError(f"Operation {name} is not a {kind.value}")
        return value

    def get_step(self, name):
        """Find a named top-level STEP operation; raise ValueError if missing or of another
        type.
        """
        return self._find(name, OperationType.STEP)

    def get_wait(self, name):
        """Find a named top-level WAIT operation; raise ValueError if missing or of another
        type.
        """
        return self._find(name, OperationType.WAIT)

    def get_callback(self, name):
        """Find a named top-level CALLBACK operation; raise ValueError if missing or of
        another type.
        """
        return self._find(name, OperationType.CALLBACK)

    def get_context(self, name):
        """Find a named top-level CONTEXT operation; raise ValueError if missing or of
        another type.
        """
        return self._find(name, OperationType.CONTEXT)

    def get_invoke(self, name):
        """Find a named top-level CHAINED_INVOKE operation; raise ValueError if missing or
        of another type.
        """
        return self._find(name, OperationType.CHAINED_INVOKE)

    def get_execution(self, name):
        """Find a root EXECUTION record by name; raise ValueError when no matching record
        exists.
        """
        for item in self._all_operations:
            if item.name == name and item.operation_type is OperationType.EXECUTION:
                return item
        raise ValueError(f"Execution not found: {name}")

    def get_deserialized_result(self, serdes=None):
        """Decode the final serialized execution result.

        Args:
            serdes (ExtendedTypeSerDes | None): Codec exposing deserialize_sync(); None
                uses the default typed codec, which also accepts ordinary JSON.

        Returns:
            (Any): Decoded value, or None for a missing or empty payload.
        """
        return deserialize(self.result, serdes)

    def get_operation_deserialized_result(self, operation, serdes=None):
        """Decode a step, child-context, callback, or chained-invocation result payload.

        Args:
            operation (Any): Operation view obtained from this result's lookup methods.
            serdes (ExtendedTypeSerDes | None): Codec exposing deserialize_sync(); None
                selects the default typed/JSON decoder.

        Returns:
            (Any): Decoded stored payload, or None when the operation has no result
                payload.
        """
        for attr in (
            "step_details",
            "context_details",
            "callback_details",
            "chained_invoke_details",
        ):
            details = getattr(operation, attr, None)
            if details is not None:
                return deserialize(details.result, serdes)
        return None

    def get_all_operations(self):
        """Return visible top-level and nested operation views, excluding root execution
        records.
        """
        return [
            op
            for op in self._all_operations or self.operations
            if op.operation_type is not OperationType.EXECUTION
        ]

    def get_child_operations(self, operation):
        """Return the visible direct children of the supplied operation view."""
        return [
            op
            for op in self.get_all_operations()
            if op.parent_id == operation.operation_id
        ]


def history_entries(events):
    entries: dict[str, Entry] = {}
    kinds = {
        "Execution": "EXECUTION",
        "Context": "CONTEXT",
        "Step": "STEP",
        "Wait": "WAIT",
        "Callback": "CALLBACK",
        "ChainedInvoke": "CHAINED_INVOKE",
    }
    statuses = {
        "Started": "STARTED",
        "Pending": "PENDING",
        "Succeeded": "SUCCEEDED",
        "Failed": "FAILED",
        "TimedOut": "TIMED_OUT",
        "Stopped": "STOPPED",
        "Cancelled": "CANCELLED",
    }
    for raw in sorted((mapping(e) for e in events), key=lambda e: e.get("EventId", 0)):
        event_type = raw.get("EventType", "")
        key = raw.get("Id", raw.get("OperationId"))
        if not key:
            continue
        match = next(
            ((p, s) for p in kinds for s in statuses if event_type == p + s), None
        )
        if match is None:
            continue
        prefix, suffix = match
        entry = entries.setdefault(key, Entry(key, kinds[prefix], statuses[suffix]))
        entry.status = statuses[suffix]
        entry.name = raw.get("Name", entry.name)
        entry.parent = raw.get("ParentId", entry.parent)
        entry.subtype = raw.get("SubType", entry.subtype)
        if suffix == "Started":
            entry.started = epoch(raw.get("EventTimestamp"))
        else:
            entry.ended = epoch(raw.get("EventTimestamp"))
        details = raw.get(event_type + "Details") or {}
        if details.get("Result"):
            entry.payload = details["Result"].get("Payload")
        if details.get("Input"):
            entry.payload = details["Input"].get("Payload")
        if details.get("Error"):
            entry.error = details["Error"].get("Payload")
        entry.callback = details.get("CallbackId", entry.callback)
        entry.due = epoch(details.get("ScheduledEndTimestamp")) or entry.due
        entry.attempt = (details.get("RetryDetails") or {}).get(
            "CurrentAttempt", entry.attempt
        )
    return entries

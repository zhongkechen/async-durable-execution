"""An append-only effect journal with one lease writer and supervised tasks.

    Workflow -> Ticket -> Journal.commit -> Backend

Tickets reserve identity synchronously. The writer owns the checkpoint lease.
Every submitted future stays registered until acknowledgement, including while
requests are being collected or sent. There are no operation executor classes.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ._scope import (
    DurableContext,
    Identity,
    SerDesContext,
    binding,
    get_durable_context,
)
from ._serde import convert
from ._types import (
    ErrorObject,
    InvalidStateError,
    InvocationError,
    OperationSubType,
    OperationType,
)


class Pause(BaseException):
    def __init__(self, until: float | None = None):
        self.until = until
        super().__init__("Workflow has durable work pending")


def mapping(value):
    return value if isinstance(value, dict) else value.to_dict()


def epoch(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    number = float(value)
    return number / 1000 if abs(number) > 100_000_000_000 else number


@dataclass
class Entry:
    key: str
    kind: str
    status: str
    name: str | None = None
    parent: str | None = None
    subtype: str | None = None
    payload: str | None = None
    error: dict | None = None
    attempt: int = 0
    due: float | None = None
    callback: str | None = None
    started: float | None = None
    ended: float | None = None
    replay_children: bool = False
    options: dict = field(default_factory=dict)

    @classmethod
    def read(cls, value):
        raw = mapping(value)
        kind = raw["Type"]
        key = {
            "EXECUTION": "ExecutionDetails",
            "CONTEXT": "ContextDetails",
            "STEP": "StepDetails",
            "WAIT": "WaitDetails",
            "CALLBACK": "CallbackDetails",
            "CHAINED_INVOKE": "ChainedInvokeDetails",
        }[kind]
        details = raw.get(key) or {}
        return cls(
            raw["Id"],
            kind,
            raw["Status"],
            raw.get("Name"),
            raw.get("ParentId"),
            raw.get("SubType"),
            details.get("InputPayload")
            if kind == "EXECUTION"
            else details.get("Result"),
            details.get("Error"),
            details.get("Attempt", 0),
            epoch(
                details.get(
                    "NextAttemptTimestamp", details.get("ScheduledEndTimestamp")
                )
            ),
            details.get("CallbackId"),
            epoch(raw.get("StartTimestamp")),
            epoch(raw.get("EndTimestamp")),
            details.get("ReplayChildren", False),
        )

    def wire(self):
        raw: dict[str, Any] = {"Id": self.key, "Type": self.kind, "Status": self.status}
        raw.update(
            {
                key: value
                for key, value in {
                    "Name": self.name,
                    "ParentId": self.parent,
                    "SubType": self.subtype,
                    "StartTimestamp": self.started,
                    "EndTimestamp": self.ended,
                }.items()
                if value is not None
            }
        )
        details: dict[str, Any] = {}
        if self.kind == "EXECUTION":
            details["InputPayload"] = self.payload
        else:
            if self.payload is not None:
                details["Result"] = self.payload
            if self.error is not None:
                details["Error"] = self.error
        if self.kind == "CONTEXT":
            details["ReplayChildren"] = self.replay_children
        if self.kind == "STEP":
            details["Attempt"] = self.attempt
            if self.due is not None:
                details["NextAttemptTimestamp"] = self.due
        if self.kind == "WAIT" and self.due is not None:
            details["ScheduledEndTimestamp"] = self.due
        if self.kind == "CALLBACK":
            details["CallbackId"] = self.callback
        raw[
            {
                "EXECUTION": "ExecutionDetails",
                "CONTEXT": "ContextDetails",
                "STEP": "StepDetails",
                "WAIT": "WaitDetails",
                "CALLBACK": "CallbackDetails",
                "CHAINED_INVOKE": "ChainedInvokeDetails",
            }[self.kind]
        ] = details
        return raw


@dataclass(frozen=True)
class Change:
    """Request adapter for user-supplied DurableServiceClient implementations."""

    value: dict

    def to_dict(self):
        return self.value.copy()

    def __getattr__(self, name):
        aliases = {
            "operation_id": "Id",
            "operation_type": "Type",
            "parent_id": "ParentId",
            "sub_type": "SubType",
        }
        value = self.value.get(
            aliases.get(name, "".join(s.title() for s in name.split("_")))
        )
        if name == "operation_type" and value:
            return OperationType(value)
        if name == "error" and value:
            return ErrorObject.from_dict(value)
        return value


class Cursor:
    def __init__(self, context: DurableContext):
        self.context = context
        self.sequence = 0
        self.locals: set[str] = set()
        self.selected = False
        self.prefix = context.operation_id_generator_prefix or "root"
        self.remaining = {
            key
            for key, entry in context.execution_state.entries.items()
            if entry.parent == context.parent_id
            and entry.kind != "EXECUTION"
            and not (entry.subtype or "").startswith("ade3-")
        }
        self.replaying = bool(self.remaining)

    def reserve(self, name=None, local_id=None):
        if local_id is None:
            address = f"{self.prefix}/sequence/{self.sequence}"
            self.sequence += 1
        else:
            if not isinstance(local_id, str) or not local_id.strip():
                raise ValueError("local_operation_id must be a nonblank string")
            if self.selected:
                raise RuntimeError("Reserve local IDs before selecting an operation")
            if local_id in self.locals:
                raise ValueError("local_operation_id is already reserved")
            self.locals.add(local_id)
            address = f"{self.prefix}/named/{local_id}"
        key = hashlib.sha256(address.encode()).hexdigest()
        return Ticket(self, key, name)


def cursor(context=None):
    context = context or get_durable_context()
    if "_cursor" not in context.__dict__:
        context.__dict__["_cursor"] = Cursor(context)
    return context.__dict__["_cursor"]


class Ticket:
    def __init__(self, owner, key, name):
        self.owner, self.key, self.name = owner, key, name
        self.context = get_durable_context()
        self.journal = owner.context.execution_state
        self.kind = self.subtype = None
        self.claimed = False
        self.parent = owner.context.parent_id

    def select(self, kind, subtype):
        if self.claimed:
            raise RuntimeError("A reservation can only be selected once")
        if get_durable_context() is not self.context:
            raise RuntimeError("Reservation belongs to another durable context")
        self.claimed = self.owner.selected = True
        self.kind = kind.value if isinstance(kind, OperationType) else kind
        self.subtype = (
            subtype.value if isinstance(subtype, OperationSubType) else subtype
        )
        saved = self.entry
        if saved and (saved.kind, saved.subtype, saved.name, saved.parent) != (
            self.kind,
            self.subtype,
            self.name,
            self.parent,
        ):
            raise InvalidStateError(
                f"Operation {self.name or self.key} changed its type, subtype, name or parent during replay"
            )
        if saved is None:
            self.owner.replaying = False
        self.owner.remaining.discard(self.key)
        return self

    @property
    def entry(self):
        return self.journal.entries.get(self.key)

    @property
    def identity(self):
        return Identity(self.key, self.subtype, self.parent, self.name)

    def metadata(self, attempt=None, original=None):
        subtype: Any
        try:
            subtype = OperationSubType(self.subtype)
        except ValueError:
            subtype = self.subtype
        return SerDesContext(
            self.key,
            self.journal.durable_execution_arn,
            self.journal.recursive_level,
            f"operation/{self.key}",
            self.name,
            self.parent,
            OperationType(self.kind),
            subtype,
            attempt,
            original,
        )

    async def encode(self, value, codec=None, attempt=None):
        return await convert(codec, "serialize", value, self.metadata(attempt, value))

    async def decode(self, payload, codec=None, attempt=None):
        return await convert(codec, "deserialize", payload, self.metadata(attempt))

    async def write(self, action, **fields):
        request = {
            "Id": self.key,
            "Type": self.kind,
            "Action": action,
            "SubType": self.subtype,
        }
        if self.name is not None:
            request["Name"] = self.name
        if self.parent is not None:
            request["ParentId"] = self.parent
        request.update(fields)
        await self.journal.commit(request)
        if not self.owner.remaining:
            self.owner.replaying = False
        return self.entry

    def child(self, *, virtual=False, kind=DurableContext, **extra):
        return kind(
            self.journal,
            Identity(None, parent_id=self.parent if virtual else self.key),
            self.key,
            bool(self.entry),
            **extra,
        )

    def spawn(self, operation):
        return self.journal.spawn(operation)


class Journal:
    def __init__(self, arn, token, backend, entries=(), lambda_context=None):
        self.durable_execution_arn, self.token, self.backend = arn, token, backend
        self.lambda_context = lambda_context
        self.entries = {item.key: item for item in entries}
        self.tasks: set[asyncio.Task] = set()
        self.mailbox: asyncio.Queue = asyncio.Queue()
        self.pending: set[asyncio.Future] = set()
        self.writer: asyncio.Task | None = None
        self.failure: BaseException | None = None
        self.closed = False
        self.input = next(
            (entry.payload for entry in entries if entry.kind == "EXECUTION"), "null"
        )
        try:
            parsed = json.loads(self.input or "null")
            self.recursive_level = (
                int(parsed.get("__recursive_level", 0))
                if isinstance(parsed, dict)
                else 0
            )
        except (ValueError, TypeError):
            self.recursive_level = 0

    def spawn(self, operation):
        if self.closed:
            operation.close()
            raise InvalidStateError("Invocation is already closed")
        task = asyncio.create_task(operation)
        self.tasks.add(task)

        def settled(task):
            self.tasks.discard(task)
            if not task.cancelled():
                task.exception()

        task.add_done_callback(settled)
        return task

    async def commit(self, update):
        if self.failure:
            raise self.failure
        if self.closed:
            raise InvalidStateError("Cannot checkpoint after invocation close")
        loop = asyncio.get_running_loop()
        receipt = loop.create_future()
        self.pending.add(receipt)
        self.mailbox.put_nowait((update, receipt))
        if self.writer is None:
            self.writer = loop.create_task(self._write_loop())
            self.writer.add_done_callback(self._writer_done)
        try:
            return await asyncio.shield(receipt)
        finally:
            if receipt.done():
                self.pending.discard(receipt)
                if not receipt.cancelled():
                    receipt.exception()

    def _writer_done(self, task):
        if task.cancelled():
            self._break(InvocationError("Checkpoint writer interrupted"))
        elif task.exception() is not None:
            self._break(task.exception())

    def _break(self, error):
        self.failure = error
        for receipt in tuple(self.pending):
            if not receipt.done():
                receipt.set_exception(error)
                receipt.exception()
        self.pending.clear()

    async def _write_loop(self):
        try:
            while True:
                first = await self.mailbox.get()
                if first is None:
                    return
                batch = [first]
                total = len(json.dumps(first[0], ensure_ascii=False).encode())
                # Batch only already-ready writes. No timer owns invisible receipts.
                while len(batch) < 100 and total < 400_000 and not self.mailbox.empty():
                    following = self.mailbox.get_nowait()
                    if following is None:
                        self.mailbox.put_nowait(None)
                        break
                    batch.append(following)
                    total += len(json.dumps(following[0], ensure_ascii=False).encode())
                response = await self.backend.transact(
                    self.token, [change for change, _ in batch]
                )
                self.token = response.get("CheckpointToken", self.token)
                for record in response.get("NewExecutionState", {}).get(
                    "Operations", ()
                ):
                    entry = Entry.read(record)
                    self.entries[entry.key] = entry
                marker = response.get("NewExecutionState", {}).get("NextMarker")
                while marker:
                    page = await self.backend.page(self.token, marker)
                    for record in page.get("Operations", ()):
                        entry = Entry.read(record)
                        self.entries[entry.key] = entry
                    marker = page.get("NextMarker")
                for _, receipt in batch:
                    if not receipt.done():
                        receipt.set_result(None)
                    self.pending.discard(receipt)
        except asyncio.CancelledError:
            self._break(InvocationError("Checkpoint writer interrupted"))
            raise
        except BaseException as error:
            self._break(error)

    async def close(self, abort=False):
        while True:
            running = [
                task
                for task in self.tasks
                if task is not asyncio.current_task() and not task.done()
            ]
            if not running:
                break
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
        self.closed = True
        if self.writer is not None:
            self.mailbox.put_nowait(None)
            if abort:
                self.writer.cancel()
            try:
                await self.writer
            except asyncio.CancelledError:
                if not self.writer.cancelled():
                    raise

    async def store(self, ticket, payload):
        if len(payload.encode("utf-8")) <= 200_000:
            return payload, False
        keys = []
        for index, offset in enumerate(range(0, len(payload), 32768)):
            key = hashlib.sha256(f"{ticket.key}/payload/{index}".encode()).hexdigest()
            keys.append(key)
            part = payload[offset : offset + 32768]
            saved = self.entries.get(key)
            if saved:
                if saved.payload != part:
                    raise InvalidStateError("Result changed after partial persistence")
                continue
            for action, extra in (("START", {}), ("SUCCEED", {"Payload": part})):
                await self.commit(
                    {
                        "Id": key,
                        "Type": "STEP",
                        "Action": action,
                        "SubType": "ade3-payload",
                        "ParentId": ticket.key,
                        **extra,
                    }
                )
        manifest = json.dumps({"adeChunks": 3, "keys": keys}, separators=(",", ":"))
        if len(manifest) > 200_000:
            raise InvalidStateError("Result chunk manifest exceeds checkpoint capacity")
        return manifest, True

    def restore(self, payload):
        try:
            document = json.loads(payload)
        except (TypeError, ValueError):
            return payload
        if not isinstance(document, dict) or document.get("adeChunks") != 3:
            return payload
        parts = []
        for key in document["keys"]:
            saved = self.entries.get(key)
            if saved is None or saved.status != "SUCCEEDED" or saved.payload is None:
                raise InvalidStateError(
                    "A completed result references a missing journal chunk"
                )
            parts.append(saved.payload)
        return "".join(parts)

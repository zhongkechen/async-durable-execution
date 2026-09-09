"""Identical fake Lambda state transitions behind sync and async adapters."""

import asyncio
import copy
import datetime
import json
import threading
import time

NOW = datetime.datetime(2026, 9, 9, tzinfo=datetime.timezone.utc)
ARN = "arn:aws:lambda:us-west-2:123456789012:function:benchmark:1/durable-execution/run/root"
CONTEXT = type(
    "LambdaContext",
    (),
    {
        "aws_request_id": "benchmark",
        "function_name": "benchmark",
        "function_version": "1",
        "invoked_function_arn": "arn:aws:lambda:us-west-2:123456789012:function:benchmark:1",
        "tenant_id": None,
        "memory_limit_in_mb": 1024,
        "log_group_name": "benchmark",
        "log_stream_name": "benchmark",
        "get_remaining_time_in_millis": lambda self: 900000,
    },
)()


class Backend:
    """Record STEP/CONTEXT updates using the same reducer for both APIs."""

    def __init__(self, latency: float = 0):
        self.latency = latency
        self.lock = threading.RLock()
        self.reset()

    def reset(self) -> None:
        self.ops: dict[str, dict] = {
            "root": {
                "Id": "root",
                "Type": "EXECUTION",
                "Status": "STARTED",
                "StartTimestamp": NOW,
                "ExecutionDetails": {"InputPayload": "{}"},
            }
        }
        self.token = "token-0"
        self.calls = self.updates = self.request_bytes = self.get_calls = (
            self.effects
        ) = 0
        self.trace: list[dict] = []
        self.started = time.perf_counter()

    def effect(self) -> None:
        with self.lock:
            self.effects += 1

    def apply(self, request: dict) -> dict:
        with self.lock:
            if request["CheckpointToken"] != self.token:
                raise ValueError("Stale checkpoint token")
            self.calls += 1
            self.updates += len(request.get("Updates", []))
            self.request_bytes += len(
                json.dumps(request, separators=(",", ":"), default=str).encode()
            )
            self.trace.append(
                {
                    "ms": (time.perf_counter() - self.started) * 1000,
                    "actions": [
                        u["Type"] + ":" + u["Action"]
                        for u in request.get("Updates", [])
                    ],
                }
            )
            touched = {}
            for change in request.get("Updates", []):
                key, kind, action = change["Id"], change["Type"], change["Action"]
                if kind not in ("STEP", "CONTEXT"):
                    raise ValueError("Unexpected benchmark operation type: " + kind)
                record = self.ops.get(
                    key,
                    {
                        "Id": key,
                        "Type": kind,
                        "Status": "STARTED",
                        "StartTimestamp": NOW,
                    },
                )
                for attr in ("Name", "ParentId", "SubType"):
                    if attr in change:
                        record[attr] = change[attr]
                field = "StepDetails" if kind == "STEP" else "ContextDetails"
                detail = record.setdefault(
                    field,
                    {"Attempt": 0} if kind == "STEP" else {"ReplayChildren": False},
                )
                record["Status"] = {
                    "START": "STARTED",
                    "SUCCEED": "SUCCEEDED",
                    "FAIL": "FAILED",
                }[action]
                if action != "START":
                    record["EndTimestamp"] = NOW
                if "Payload" in change:
                    detail["Result"] = change["Payload"]
                if "Error" in change:
                    detail["Error"] = change["Error"]
                if kind == "STEP" and action != "START":
                    detail["Attempt"] += 1
                if "ContextOptions" in change:
                    detail["ReplayChildren"] = change["ContextOptions"].get(
                        "ReplayChildren", False
                    )
                self.ops[key] = record
                touched[key] = record
            self.token = f"token-{self.calls}"
            return {
                "CheckpointToken": self.token,
                "NewExecutionState": {
                    "Operations": copy.deepcopy(list(touched.values()))
                },
            }

    def get_state(self, request: dict) -> dict:
        with self.lock:
            self.get_calls += 1
            return {"Operations": copy.deepcopy(list(self.ops.values()))}

    def event(self) -> dict:
        def encode(obj):
            if isinstance(obj, datetime.datetime):
                return int(obj.timestamp() * 1000)
            raise TypeError(type(obj).__name__)

        operations = json.loads(json.dumps(list(self.ops.values()), default=encode))
        return {
            "DurableExecutionArn": ARN,
            "CheckpointToken": self.token,
            "InitialExecutionState": {"Operations": operations},
        }


class AsyncBackend(Backend):
    async def checkpoint_durable_execution(self, **request) -> dict:
        if self.latency:
            await asyncio.sleep(self.latency)
        return self.apply(request)

    async def get_durable_execution_state(self, **request) -> dict:
        return self.get_state(request)


class SyncBackend(Backend):
    def checkpoint_durable_execution(self, **request) -> dict:
        if self.latency:
            time.sleep(self.latency)
        return self.apply(request)

    def get_durable_execution_state(self, **request) -> dict:
        return self.get_state(request)

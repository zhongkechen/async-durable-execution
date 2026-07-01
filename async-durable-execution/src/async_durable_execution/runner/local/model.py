"""Models used only by the local durable execution runner."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Protocol

from async_durable_execution.execution import DurableExecutionInvocationInput
from async_durable_execution.models import (
    CheckpointUpdatedExecutionState,
    LambdaContext as LambdaContextProtocol,
    Operation,
)
from async_durable_execution.runner.exceptions import InvalidParameterValueException
from async_durable_execution.runner.model import InvokeResponse


@dataclass(frozen=True)
class LambdaContext(LambdaContextProtocol):
    """Lambda context for local testing."""

    aws_request_id: str
    log_group_name: str | None = None
    log_stream_name: str | None = None
    function_name: str | None = None
    memory_limit_in_mb: str | None = None
    function_version: str | None = None
    invoked_function_arn: str | None = None
    tenant_id: str | None = None
    client_context: dict | None = None
    identity: dict | None = None

    def get_remaining_time_in_millis(self) -> int:
        return 900000

    def log(self, msg) -> None:
        pass


@dataclass(frozen=True)
class StartDurableExecutionInput:
    """Input for starting a local durable execution."""

    account_id: str
    function_name: str
    function_qualifier: str
    execution_name: str
    execution_timeout_seconds: int
    execution_retention_period_days: int
    invocation_id: str | None = None
    trace_fields: dict | None = None
    tenant_id: str | None = None
    input: str | None = None
    lambda_endpoint: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StartDurableExecutionInput:
        required_fields = [
            "AccountId",
            "FunctionName",
            "FunctionQualifier",
            "ExecutionName",
            "ExecutionTimeoutSeconds",
            "ExecutionRetentionPeriodDays",
        ]

        for field in required_fields:
            if field not in data:
                msg = f"Missing required field: {field}"
                raise InvalidParameterValueException(msg)

        return cls(
            account_id=data["AccountId"],
            function_name=data["FunctionName"],
            function_qualifier=data["FunctionQualifier"],
            execution_name=data["ExecutionName"],
            execution_timeout_seconds=data["ExecutionTimeoutSeconds"],
            execution_retention_period_days=data["ExecutionRetentionPeriodDays"],
            invocation_id=data.get("InvocationId"),
            trace_fields=data.get("TraceFields"),
            tenant_id=data.get("TenantId"),
            input=data.get("Input"),
            lambda_endpoint=data.get("LambdaEndpoint"),
        )

    def to_dict(self) -> dict[str, Any]:
        result = {
            "AccountId": self.account_id,
            "FunctionName": self.function_name,
            "FunctionQualifier": self.function_qualifier,
            "ExecutionName": self.execution_name,
            "ExecutionTimeoutSeconds": self.execution_timeout_seconds,
            "ExecutionRetentionPeriodDays": self.execution_retention_period_days,
        }
        if self.invocation_id is not None:
            result["InvocationId"] = self.invocation_id
        if self.trace_fields is not None:
            result["TraceFields"] = self.trace_fields
        if self.tenant_id is not None:
            result["TenantId"] = self.tenant_id
        if self.input is not None:
            result["Input"] = self.input
        if self.lambda_endpoint is not None:
            result["LambdaEndpoint"] = self.lambda_endpoint
        return result

    def get_normalized_input(self):
        """Normalize input string to be JSON deserializable."""
        try:
            json.loads(self.input)
            return self.input
        except (json.JSONDecodeError, TypeError):
            return json.dumps(self.input)


@dataclass(frozen=True)
class StartDurableExecutionOutput:
    """Output from starting a local durable execution."""

    execution_arn: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> StartDurableExecutionOutput:
        return cls(execution_arn=data.get("ExecutionArn"))

    def to_dict(self) -> dict[str, Any]:
        result = {}
        if self.execution_arn is not None:
            result["ExecutionArn"] = self.execution_arn
        return result


@dataclass(frozen=True)
class GetDurableExecutionStateResponse:
    """Local response containing durable execution state operations."""

    operations: list[Operation]
    next_marker: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> GetDurableExecutionStateResponse:
        operations = [
            Operation.from_dict(op_data) for op_data in data.get("Operations", [])
        ]
        return cls(
            operations=operations,
            next_marker=data.get("NextMarker"),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "Operations": [op.to_dict() for op in self.operations]
        }
        if self.next_marker is not None:
            result["NextMarker"] = self.next_marker
        return result


@dataclass(frozen=True)
class SendDurableExecutionCallbackSuccessResponse:
    """Response from sending local callback success."""


@dataclass(frozen=True)
class SendDurableExecutionCallbackFailureResponse:
    """Response from sending local callback failure."""


@dataclass(frozen=True)
class SendDurableExecutionCallbackHeartbeatResponse:
    """Response from sending local callback heartbeat."""


@dataclass(frozen=True)
class CheckpointDurableExecutionResponse:
    """Local response from checkpointing a durable execution."""

    checkpoint_token: str
    new_execution_state: CheckpointUpdatedExecutionState | None = None

    @classmethod
    def from_dict(cls, data: dict) -> CheckpointDurableExecutionResponse:
        new_execution_state = None
        if state_data := data.get("NewExecutionState"):
            new_execution_state = CheckpointUpdatedExecutionState.from_dict(state_data)

        return cls(
            checkpoint_token=data["CheckpointToken"],
            new_execution_state=new_execution_state,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"CheckpointToken": self.checkpoint_token}
        if self.new_execution_state is not None:
            result["NewExecutionState"] = self.new_execution_state.to_dict()
        return result


class Invoker(Protocol):
    def create_invocation_input(
        self,
        *,
        start_input: StartDurableExecutionInput,
        durable_execution_arn: str,
        checkpoint_token: str,
        operations: list[Operation],
    ) -> DurableExecutionInvocationInput: ...  # pragma: no cover

    async def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,
        endpoint_url: str | None = None,
    ) -> InvokeResponse: ...  # pragma: no cover

    def update_endpoint(
        self, endpoint_url: str, region_name: str
    ) -> None: ...  # pragma: no cover


@dataclass(frozen=True)
class CheckpointToken:
    """Model a local checkpoint token."""

    execution_arn: str
    token_sequence: int

    def to_str(self) -> str:
        data = {"arn": self.execution_arn, "seq": self.token_sequence}
        json_str = json.dumps(data, separators=(",", ":"))
        return base64.b64encode(json_str.encode()).decode()

    @classmethod
    def from_str(cls, token: str) -> CheckpointToken:
        decoded = base64.b64decode(token).decode()
        data = json.loads(decoded)
        return cls(execution_arn=data["arn"], token_sequence=data["seq"])


@dataclass(frozen=True)
class CallbackToken:
    """Model a local callback token."""

    execution_arn: str
    operation_id: str

    def to_str(self) -> str:
        data = {"arn": self.execution_arn, "op": self.operation_id}
        json_str = json.dumps(data, separators=(",", ":"))
        return base64.b64encode(json_str.encode()).decode()

    @classmethod
    def from_str(cls, token: str) -> CallbackToken:
        decoded = base64.b64decode(token).decode()
        data = json.loads(decoded)
        return cls(execution_arn=data["arn"], operation_id=data["op"])


__all__ = [
    "CallbackToken",
    "CheckpointDurableExecutionResponse",
    "CheckpointToken",
    "GetDurableExecutionStateResponse",
    "Invoker",
    "LambdaContext",
    "SendDurableExecutionCallbackFailureResponse",
    "SendDurableExecutionCallbackHeartbeatResponse",
    "SendDurableExecutionCallbackSuccessResponse",
    "StartDurableExecutionInput",
    "StartDurableExecutionOutput",
]

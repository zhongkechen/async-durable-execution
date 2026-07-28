"""Models used only by the local durable execution runner."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

from ..._core import (
    BotoSerializableModel,
    CheckpointUpdatedExecutionState,
    DurableExecutionInvocationInput,
    LambdaContext as LambdaContextProtocol,
    Operation,
)
from ..exceptions import InvalidParameterValueException
from ..model import InvokeResponse


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
class StartDurableExecutionInput(BotoSerializableModel):
    """Input for starting a local durable execution."""

    account_id: str = field(metadata={"alias": "AccountId"})
    function_name: str = field(metadata={"alias": "FunctionName"})
    function_qualifier: str = field(metadata={"alias": "FunctionQualifier"})
    execution_name: str = field(metadata={"alias": "ExecutionName"})
    execution_timeout_seconds: int = field(
        metadata={"alias": "ExecutionTimeoutSeconds"}
    )
    execution_retention_period_days: int = field(
        metadata={"alias": "ExecutionRetentionPeriodDays"}
    )
    invocation_id: str | None = field(default=None, metadata={"alias": "InvocationId"})
    trace_fields: dict | None = field(default=None, metadata={"alias": "TraceFields"})
    tenant_id: str | None = field(default=None, metadata={"alias": "TenantId"})
    input: str | None = field(default=None, metadata={"alias": "Input"})
    lambda_endpoint: str | None = field(
        default=None, metadata={"alias": "LambdaEndpoint"}
    )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> StartDurableExecutionInput:
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

        return super().from_dict(data)

    def get_normalized_input(self) -> str:
        """Normalize input string to be JSON deserializable."""
        try:
            json.loads(cast(str, self.input))
            return cast(str, self.input)
        except (json.JSONDecodeError, TypeError):
            return json.dumps(self.input)


@dataclass(frozen=True)
class StartDurableExecutionOutput(BotoSerializableModel):
    """Output from starting a local durable execution."""

    execution_arn: str | None = field(default=None, metadata={"alias": "ExecutionArn"})


@dataclass(frozen=True)
class GetDurableExecutionStateResponse(BotoSerializableModel):
    """Local response containing durable execution state operations."""

    operations: list[Operation] = field(
        default_factory=list, metadata={"alias": "Operations"}
    )
    next_marker: str | None = field(default=None, metadata={"alias": "NextMarker"})


@dataclass(frozen=True)
class SendDurableExecutionCallbackSuccessResponse(BotoSerializableModel):
    """Response from sending local callback success."""


@dataclass(frozen=True)
class SendDurableExecutionCallbackFailureResponse(BotoSerializableModel):
    """Response from sending local callback failure."""


@dataclass(frozen=True)
class SendDurableExecutionCallbackHeartbeatResponse(BotoSerializableModel):
    """Response from sending local callback heartbeat."""


@dataclass(frozen=True)
class CheckpointDurableExecutionResponse(BotoSerializableModel):
    """Local response from checkpointing a durable execution."""

    checkpoint_token: str | None = field(
        default=None, metadata={"alias": "CheckpointToken"}
    )
    new_execution_state: CheckpointUpdatedExecutionState | None = field(
        default=None, metadata={"alias": "NewExecutionState"}
    )


class Invoker(Protocol):
    def create_invocation_input(
        self,
        *,
        start_input: StartDurableExecutionInput,
        durable_execution_arn: str,
        checkpoint_token: str,
        operations: list[Operation],
    ) -> DurableExecutionInvocationInput: ...

    async def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,
        endpoint_url: str | None = None,
    ) -> InvokeResponse: ...


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

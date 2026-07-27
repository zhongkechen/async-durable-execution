# Core API

Import these public symbols from `async_durable_execution`.

## Execution

Handler input is deserialized from the durable execution payload before user
code runs. Empty or whitespace payloads are normalized to `{}`, and malformed
JSON fails the invocation before the handler executes.

::: async_durable_execution
    options:
      members:
        - durable_callable
        - durable_execution

## Current Context

Use `get_current_context()` only while supported durable user code is running.

::: async_durable_execution
    options:
      members:
        - DurableContext
        - get_current_context

## Configuration

::: async_durable_execution
    options:
      members:
        - JitterStrategy
        - RetryStrategy

## Serialization

::: async_durable_execution
    options:
      members:
        - SerDesContext
        - SerDes
        - JsonSerDes
        - ExtendedTypeSerDes

## Models

::: async_durable_execution
    options:
      members:
        - LambdaContext
        - OperationStatus
        - OperationSubType
        - OperationType
        - InvocationStatus
        - ErrorObject

## Exceptions

::: async_durable_execution
    options:
      members:
        - DurableExecutionsError
        - ExecutionError
        - InvocationError
        - ValidationError
        - InvalidStateError
        - UserlandError
        - CallableRuntimeError
        - SerDesError

## Durable Service Client

::: async_durable_execution
    options:
      members:
        - DurableServiceClient
        - create_default_sync_client

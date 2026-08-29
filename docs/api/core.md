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

Use context getters only while the corresponding durable user code is running.
Prefer the specific getter for the active scope: it validates the runtime
context and gives type checkers the concrete context type without a cast.

| Execution scope | Getter | Context type |
| --- | --- | --- |
| Durable handler, child context, or parallel branch | `get_durable_context()` | `DurableContext` |
| Step function | `get_step_context()` | `StepContext` |
| Map item function | `get_map_item_context()` | `MapItemContext` |
| Flow node | `get_node_context()` | `FlowNodeContext` |
| `with_retry` body | `get_with_retry_context()` | `WithRetryContext` |
| `wait_for_callback` submitter | `get_wait_for_callback_context()` | `WaitForCallbackContext` |
| `wait_for_condition` check | `get_wait_for_condition_check_context()` | `WaitForConditionCheckContext` |
| Serializer or deserializer | `get_serdes_context()` | `SerDesContext` |

`get_current_context()` remains available when code intentionally handles more
than one context type.

::: async_durable_execution
    options:
      members:
        - DurableContext
        - get_current_context
        - get_durable_context

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
        - get_serdes_context
        - SerDes
        - SerDesStage
        - ComposableSerDes
        - create_serdes_pipeline
        - is_composable_serdes
        - JsonSerDes
        - ExtendedTypeSerDes
        - FileSystemSerDesStage
        - FileSystemSerDesStageConfig
        - FileSystemSerDesMode
        - FileSystemPathEncoding
        - create_file_system_serdes_stage
        - PreviewMode
        - FieldMatchMode
        - PreviewField
        - PreviewConfig
        - build_preview

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
        - RetryableSerDesError
        - SerDesError
        - SerDesPipelineError

## Durable Service Client

::: async_durable_execution
    options:
      members:
        - DurableServiceClient
        - create_default_sync_client

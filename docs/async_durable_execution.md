# API Reference

Import the supported public symbols from `async_durable_execution`. The
reference pages below are grouped by responsibility, and each page documents
exactly one implementation module.

## Primitive Operations

These operations map directly to durable execution backend operations.

- [Step](api/primitive/step.md)
- [Wait](api/primitive/wait.md)
- [Invoke](api/primitive/invoke.md)
- [Child context](api/primitive/child.md)
- [Callback](api/primitive/callback.md)

## Extension Operations

These SDK operations are implemented on top of primitive operations or SDK
checkpoint conventions.

- [Flow and DAG workflows](api/extension/flow.md)
- [Map](api/extension/map.md)
- [Parallel](api/extension/parallel.md)
- [Wait for callback](api/extension/wait_for_callback.md)
- [Wait for condition](api/extension/wait_for_condition.md)
- [Retry durable work](api/extension/with_retry.md)
- [Recursive invocation](api/extension/recurse.md)
- [Replay-safe values](api/extension/replay_safe.md)

## Core Runtime

- [Execution decorators](api/execution.md)
- [Current context](api/context.md)

## Configuration and Data

- [Configuration](api/config.md)
- [Serialization](api/serdes.md)
- [Models](api/models.md)

## Errors

- [Exceptions](api/exceptions.md)

## Integrations

- [Durable service client](api/client.md)

## Testing

- [Local runner](api/runner/local.md)
- [Cloud runner](api/runner/cloud.md)
- [Test results](api/runner/model.md)

## Package Metadata

- [Version](api/about.md)

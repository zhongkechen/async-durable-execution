# API Reference

Import the supported public symbols from `async_durable_execution`. The
reference pages below are grouped by responsibility. Operation pages correspond
to their implementation modules; supporting APIs are grouped by package.

The implementation packages are private. Import every supported symbol from
`async_durable_execution`.

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

- [Custom operation SPI](api/extension/custom_operations.md)
- [Flow and DAG workflows](api/extension/flow.md)
- [Map](api/extension/map.md)
- [Parallel](api/extension/parallel.md)
- [Wait for callback](api/extension/wait_for_callback.md)
- [Wait for condition](api/extension/wait_for_condition.md)
- [Retry durable work](api/extension/with_retry.md)
- [Durable terminal scope](api/extension/terminal_scope.md)
- [Recursive invocation](api/extension/recurse.md)
- [Replay-safe values](api/extension/replay_safe.md)

## Core API

- [SerDes pipelines and filesystem storage](serdes-pipelines.md)
- [Execution, context, configuration, serialization, models, exceptions, and
  service client](api/core.md)

## Runner API

- [Local runner, cloud runner, and test results](api/runner.md)

## Package Metadata

- [Version](api/about.md)

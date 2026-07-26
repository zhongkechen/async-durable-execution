# API Reference

The package exports the supported public API from `async_durable_execution`.
Start with the handler decorators and context helpers below, then use the
task-oriented API sections.

- [Durable operations](api/operations.md)
- [DAG workflows](api/dag.md)
- [Composition](api/composition.md)
- [Configuration and serialization](api/configuration.md)
- [Testing](async_durable_execution/runner.md)

::: async_durable_execution
    options:
      members:
        - durable_execution
        - durable_callable
        - get_current_context
        - get_node_context
        - get_step_context
        - DurableContext
        - StepContext
        - MapItemContext
        - WaitForCallbackContext
        - WaitForConditionCheckContext
        - WithRetryContext

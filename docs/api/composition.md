# Composition

Use these operations for fan-out, polling, external callbacks, and reusable
groups of durable work. For declarative acyclic workflows, see
[DAG workflows](dag.md).

::: async_durable_execution
    options:
      members:
        - map
        - parallel
        - BatchResult
        - BatchItem
        - BatchItemStatus
        - CompletionConfig
        - CompletionStatus
        - CompletionReason
        - CompletionDecision
        - wait_for_condition
        - wait_for_callback
        - with_retry

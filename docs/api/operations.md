# Durable Operations

Durable operations checkpoint nondeterministic work or suspend execution. Give
operations stable names so execution history and tests remain readable. For
declarative composition of these operations, see [DAG workflows](dag.md).

::: async_durable_execution
    options:
      members:
        - step
        - wait
        - StepSemantics
        - invoke
        - recurse
        - run_in_child_context
        - create_callback
        - Callback
        - CallbackError
        - now
        - timestamp
        - uuid
        - random

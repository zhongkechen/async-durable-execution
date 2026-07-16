# Durable Operations

Durable operations checkpoint nondeterministic work or suspend execution. Give
operations stable names so execution history and tests remain readable.

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

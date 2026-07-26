# Wait for Condition

Implementation module: `async_durable_execution.extension.wait_for_condition`.

Use `wait_for_condition()` to checkpoint polling state between deterministic
condition checks.

::: async_durable_execution.extension.wait_for_condition
    options:
      members:
        - WaitForConditionError
        - PollingStrategy
        - wait_for_condition
        - WaitForConditionCheckContext

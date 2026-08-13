# Wait for Condition

Internal implementation module: `async_durable_execution._operation.wait_for_condition`.

Use `wait_for_condition()` to checkpoint polling state between deterministic
condition checks.

::: async_durable_execution._operation.wait_for_condition
    options:
      members:
        - WaitForConditionError
        - PollingStrategy
        - wait_for_condition
        - WaitForConditionCheckContext
        - get_wait_for_condition_check_context

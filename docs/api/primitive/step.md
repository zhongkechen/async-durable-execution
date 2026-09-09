# Step


Use `step()` to checkpoint nondeterministic work and side effects. Completed
steps return their saved result during replay instead of running again.

::: async_durable_execution
    options:
      members:
        - StepInterruptedError
        - StepSemantics
        - step
        - StepContext
        - get_step_context

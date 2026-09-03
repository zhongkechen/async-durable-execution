# Durable Terminal Scope

Internal implementation module:
`async_durable_execution._operation.terminal_scope`.

Use `terminal_scope()` for cleanup and compensation that must run at logical
durable completion rather than Python stack unwinding. See the
[terminal-scope guide](../../terminal-scopes.md) for lifecycle, replay,
ordering, cancellation, and idempotency guidance.

::: async_durable_execution._operation.terminal_scope
    options:
      members:
        - TerminalFailurePhase
        - TerminalFailure
        - TerminalScopeError
        - TerminalScopeConfig
        - DurableTerminalActions
        - terminal_scope

# Durable Terminal Scope

Import these public symbols from `async_durable_execution`.

Use `terminal_scope()` for cleanup and compensation that must run at logical
durable completion rather than Python stack unwinding. See the
[terminal-scope guide](../../terminal-scopes.md) for lifecycle, replay,
ordering, cancellation, and idempotency guidance.

::: async_durable_execution
    options:
      members:
        - TerminalFailurePhase
        - TerminalFailure
        - TerminalScopeError
        - TerminalScopeConfig
        - DurableTerminalActions
        - terminal_scope

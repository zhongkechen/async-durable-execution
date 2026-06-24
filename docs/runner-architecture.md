# Runner Architecture

The runner package has two execution paths:

- **Local testing** - `DurableFunctionLocalTestRunner` runs the handler in process and injects `InMemoryServiceClient` so checkpoint operations are processed locally.
- **Cloud testing** - `DurableFunctionCloudTestRunner` invokes a qualified Lambda function and polls for durable execution completion.

Local execution flows through these major components:

1. `DurableTestRunner` starts execution through `Executor`.
2. `Executor` creates an `Execution` and schedules the initial invocation.
3. During execution, checkpoint updates are handled by `CheckpointProcessor`.
4. Operation-specific validators and processors transform updates into step, wait, callback, context, and execution operations.
5. `ExecutionNotifier` publishes lifecycle events.
6. `Executor` observes those events and updates execution state until completion.
7. `DurableFunctionTestResult` exposes status, result payloads, and named operation lookup helpers.

Architecture diagrams live with the runner package:

- [Durable Functions Python Test Framework Architecture](../async-durable-execution-runner/assets/dar-python-test-framework-architecture.svg)
- [Event Flow Sequence Diagram](../async-durable-execution-runner/assets/dar-python-test-framework-event-flow.svg)

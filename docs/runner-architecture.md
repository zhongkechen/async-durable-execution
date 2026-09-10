# Runner Architecture

The runner package has two execution paths:

- **Local testing** - `DurableFunctionLocalTestRunner` runs the handler in process and injects `InMemoryServiceClient` so checkpoint operations are processed locally.
- **Cloud testing** - `DurableFunctionCloudTestRunner` invokes a qualified Lambda function and polls for durable execution completion.

Local execution flows through these major components:

1. `DurableTestRunner` starts execution through `Executor`.
2. `Executor` creates an `Execution` and schedules the initial invocation on the local asyncio event loop.
3. During execution, checkpoint updates are handled by `CheckpointProcessor`.
4. Operation-specific validators and processors transform updates into step, wait, callback, context, and execution operations.
5. `ExecutionNotifier` publishes lifecycle events.
6. `Executor` observes those events asynchronously and updates execution state until completion.
7. `DurableFunctionTestResult` exposes status, result payloads, and named operation lookup helpers.

Architecture diagrams:

- [Durable Functions Python Test Framework Architecture](assets/dar-python-test-framework-architecture.svg)
- [Event Flow Sequence Diagram](assets/dar-python-test-framework-event-flow.svg)

## Checkpoint batching

Synchronous checkpoints flush after ready updates have been collected, with one
event-loop yield to coalesce other ready producers. They do not wait for the
batch idle timer. Callers still wait for the service acknowledgement, including
START checkpoints for at-most-once steps. Asynchronous-only updates retain their
batching window. Byte/operation limits, FIFO overflow ordering, and coalescing of
empty checkpoints remain in force. Faster acknowledgement can produce more
requests for staggered workloads; the scheduler does not hold a blocked caller
just to wait for unrelated work.

With a positive `max_batch_time_seconds`, synchronous batches get the coalescing
yield described above. Setting this value to zero disables the collection
window: the collector flushes after its first queue item or initial overflow
drain, without yielding to collect more producers. Already-buffered overflow
still obeys the size and operation limits.

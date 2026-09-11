# Runner Architecture

The v3 local runner is an invocation driver over an in-memory journal backend.
It creates an execution journal, invokes the handler, and replays it after durable
work becomes ready. Durable waits and retries advance virtual time; callbacks
wait for user input or their deadlines. The runner timeout bounds wall time.

The handler and every child scope reserve opaque journal tickets. Effects append
state transitions through a single writer that owns the checkpoint token. Receipts
remain registered until acknowledged, so cancellation cannot lose queued waiters.
The invocation supervisor drains child tasks before closing the journal.

The cloud runner invokes deployed Lambda functions and reads paginated execution
history. Both runners expose the same public result views and callback methods.
Cloud result and callback polling tolerate temporarily missing execution history
within the caller's timeout. The local runner mirrors AWS callback semantics:
both an omitted payload and empty bytes produce a `None` callback result.

Large scope results are stored as immutable checkpoint chunks with a manifest.
Replay reads these chunks directly. It never restarts cancelled aggregate branches
to reconstruct a completed result.

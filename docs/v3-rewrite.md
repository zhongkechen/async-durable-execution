# V3 independent replacement

Version: `3.0.0.dev1`.

## Reset and source boundary

The first v3 attempt at `e035f50` retained v2 architecture and did not meet the
requested independent rewrite. Its completion claim and validation report are
withdrawn. Commit `52e40ea` removed every production Python module, the previous
implementation tests, old history fixtures, and the previous example harness.
The old commit remains recoverable on `v3-before-clean-rewrite`.

After that reset, this replacement was written using only:

- the captured public API declarations in `tests/contracts/public-api.json`;
- public usage guides and application examples;
- independent AWS Lambda service schemas supplied by botocore; and
- new tests of public calls and observable behavior.

No further reads of either previous SDK implementation or its implementation
unit tests were made. Source seen earlier in the conversation cannot be erased
from context, so this is not a formal clean-room provenance claim.

## Architecture

The replacement has 14 production Python modules. Its private modules are new;
there are no `_core`, `_operation`, `_primitive`, `_runner`, or `_extension`
packages or compatibility aliases for them.

| Component | Responsibility |
| --- | --- |
| `_journal.py` | Opaque reservations, execution journal, checkpoint lease writer, receipt tracking, task supervision, chunked result storage |
| `_effects.py` | Public effect and extension reservation APIs interpreted against journal tickets, plus Lambda invocation lifecycle |
| `_groups.py` | Bounded concurrent groups and durable completion/cancellation decisions |
| `_graph.py` | DAG compilation and a central readiness scheduler with persisted dependency choices |
| `_finalize.py` | Terminal action programs and structured failure summaries |
| `_testing.py` | In-memory journal backend and virtual-time invocation driver |
| `_remote.py` | AWS model-driven signing/transports and cloud runner |
| `_views.py` | Public inspection views over journal records |
| `_types.py`, `_scope.py`, `_serde.py` | Public values, contextual views, and value serialization |
| `_payloads.py` | Filesystem payload storage, integrity checks, and bounded structured previews |
| `__init__.py` | Top-level public exports |

A completed scope stores its result directly, using checkpoint chunks for large
values. Replay reads those chunks rather than running the scope body again.
Consequently, an oversized early-completed aggregate retains cancelled branches
without repeating their effects. DAG guard decisions persist the dependency
results selected before a node launches. Pending checkpoint receipts remain
registered throughout collection and I/O, so writer cancellation cannot lose them.

The local backend publishes callback changes with checkpoint acknowledgements,
including callbacks delivered while their submitter is still running. Filesystem
references validate ownership, path, type, size, and digest; readable paths also
include a full-owner digest to prevent identity aliasing.

## Public compatibility and migration

The 113 version-independent top-level exports retain their supported call shapes,
defaults, enum values, public dataclass fields, and public methods. Import these
symbols from `async_durable_execution`. The former `extension.py`,
`filesystem_serdes.py`, and `preview.py` modules are removed; their old submodule
import paths are not supported. Public generic
operation signatures remain available. Compatibility tests do not prescribe
private base classes, private module names, or an executor hierarchy.

The journal identities and serialized payload format are new. This reset does
not promise replay of v2 or the previous v3 attempt's histories. Existing
executions must finish on their original SDK version before migrating handlers.
Context runtime-state references are opaque implementation objects; callers
should use public context getters and the extension interface.

## Verification

The following results describe the original replacement at `1e27db6`, before the
three public submodules were consolidated. Current consolidation checks are
recorded separately below.

That suite had 323 tests: 129 public API contract checks, 96 additional
public behavior/application checks, and 98 retained packaging/tooling checks.

| Interpreter | Passed |
| --- | ---: |
| CPython 3.10.19 | 323 |
| CPython 3.11.14 | 323 |
| CPython 3.12.12 | 323 |
| CPython 3.13.5 | 323 |
| CPython 3.14.0 | 323 |
| CPython 3.15.0rc2 | 323 |

Python 3.13 combined statement/branch coverage is 81%. These are the new suite's
results; none of the prior attempt's test counts apply.

Checks include:

- replay without repeated effects; retries and stateful polling;
- nested and flat aggregates, bounded concurrency, and oversized early completion;
- callbacks, immediate delivery, heartbeat deadlines, and extension primitives;
- DAG pruning, failure routes, cloned arguments, persisted OR choices, and validation;
- terminal suspension, cancellation policies, cleanup ordering, and failure replay;
- pipeline ordering, custom type codecs, immutable filesystem writes, ownership,
  corruption, symlink rejection, and owner-path aliasing;
- transport error classification, signed HTTPX requests, and history pagination;
- repository type checking with `--check-untyped-defs`, formatting, ShellCheck, shell syntax,
  strict documentation generation, and wheel/source distribution builds;
- nine conformance SAM templates, a conformance bundle, and example deployment
  artifacts built from the replacement.

The wheel contains the typing marker and license notices and excludes every
removed private package. All 225 public API, behavior, and consumer tests also pass against an isolated
wheel installation outside the source checkout. Build CI now runs for pushes and pull requests targeting `v3` as well
as `main`.

## Module consolidation checks

After removal of the three public submodules, all 307 tests pass on CPython
3.13.5: 113 top-level API contract checks, 96 behavior/application checks, and
98 tooling checks. The 16 duplicate submodule contract checks were removed;
the captured v2 API artifact remains unchanged.

All 209 public API, behavior, and consumer tests also pass against the rebuilt
wheel outside the source checkout. The removed module names are not importable
and their files are absent from the wheel. Type checking, formatting, strict
documentation generation, and wheel/source distribution builds pass.

## Release validation

No replacement functions were deployed to AWS and no package was published.
Cloud calls are tested with mocks. Deployed Lambda conformance, production load,
and operational rollout validation remain release gates.

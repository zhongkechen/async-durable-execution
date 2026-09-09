# SDK benchmark suite

Compare the current async SDK, v2, and the official AWS Lambda Durable Execution
SDK for Python using their public Lambda handlers. This packages the experiments
reported in [issue #323](https://github.com/zhongkechen/async-durable-execution/issues/323).
It does not import private SDK APIs or copy implementation code between SDKs.

## Run

Install Git and Hatch. The benchmark Hatch environment uses CPython 3.13. Setup
needs internet access to fetch the two baseline repositories and install pinned
public dependencies. The measured workloads use a fake Lambda service and need
no AWS credentials.

From the repository root:

```bash
hatch run benchmarks:compare --output benchmark-results/full
```

Use a new output directory for each run; existing directories are never
replaced. Scratch source checkouts, wheels, and the shared runtime environment
are created inside that directory and removed on completion. Nothing changes in
your checkout, local branches, or runtime dependencies.

The pinned v2 baseline has a known incorrect replay for the large flat aggregate.
Consequently, the default comparison writes all reports and exits **1**. To keep
that result visible while allowing a successful command exit:

```bash
hatch run benchmarks:compare --allow-invalid --output benchmark-results/comparison
```

For a fast correctness smoke of the current SDK only:

```bash
hatch run benchmarks:compare --variants current --quick --output benchmark-results/smoke
```

`--quick` uses one sample, no warmup, one serializer iteration, and zero simulated
latency. Its timings are not useful performance estimates. Normal defaults are
five measured workflow/serializer samples after one warmup, 100 serializer
iterations per block, and seven fresh-process import samples after warmup.

## Source pins and isolation

| Label | Default source |
| --- | --- |
| `current` | Committed `HEAD` in this checkout (the v3 implementation now on `main`) |
| `v2` | Async repository commit `86b680f5502e29254dfa8e89cad5e310a45f0d24` |
| `official` | AWS repository commit `d61985ef697dd9144f986d837475efee91d26a3e`, from its `main` branch |

These defaults reproduce the source versions behind the comparison; `current`
advances with your checkout. Uncommitted SDK changes are not included. Override
refs to compare other committed revisions or resolve the latest official main:

```bash
hatch run benchmarks:compare --official-ref main --current-ref HEAD --v2-ref v2 \
  --output benchmark-results/latest
```

Every ref is resolved to a full commit ID and exported before measurement. Each
SDK is built into a wheel and installed into its own target directory. All
processes use the same Python executable and dependency environment from
[`requirements.txt`](requirements.txt). SDK imports are isolated from the working
checkout, user Python paths, and other SDK targets. Actual package locations and
dependency versions are checked and recorded. Each installed SDK must also pass
`pip check` against the shared dependency pins. SDKs and import probes run
sequentially; import order rotates across versions. No benchmark changes private
batching or scheduling settings.

The harness uses only the standard library; wheel building uses Hatchling 1.32.0.
The three-SDK comparison requires Python 3.11 or later because of AWS SDK support.
The Hatch command selects 3.13 for consistent results. Other supported Python
versions can run `python -m benchmarks.run` with that Hatchling version installed.

## Workloads and controls

- Ten sequential durable steps return integers 0–9; expected sum: 45.
- Twenty parallel branches each execute one durable step; expected sum: 190.
- Map integers 0–31 with concurrency eight, preserving the result list.
- A four-node diamond DAG returns 5. Official SDK results are N/A because this
  suite has no direct public API counterpart for the DAG.
- Four flat branches each return an 80,000-character string from a step; the
  aggregate must retain four items totaling 320,000 bytes on replay.
- Explicit `ExtendedTypeSerDes` round trips: 1,000 integers, 100 order dictionaries
  with Decimal prices, and a 65,536-character string.

The sync/async fake clients share the same STEP/CONTEXT state reducer. Async
step effects yield once with `asyncio.sleep(0)`; sync effects return directly.
First invocation outputs and effect counts are checked. Replay must return the
expected output without running completed effects or issuing new checkpoints.
Invalid phases are marked `INVALID` in the Markdown report and keep their raw
measurements and diagnostics in JSON. Unsupported/unselected workloads are N/A.
These checks also cover warmup runs.

The sequential workload runs at 0, 20, and 150 ms simulated latency per
checkpoint by default. Other workflows use zero latency. Options include:

```bash
hatch run benchmarks:compare --variants current official \
  --cases sequential_10 parallel_20 --latencies-ms 0 20 150 \
  --samples 10 --warmups 2 --import-samples 10 --iterations 200 \
  --timeout 600 --output benchmark-results/custom
```

`--timeout` bounds each setup or worker subprocess. Exit codes: **0** for valid
results (or explicit `--allow-invalid`), **1** for correctness failures, and **2**
for setup/execution errors. Logs and completed per-SDK measurements are retained
when setup or a worker fails. No timing threshold gates CI; the regular test
suite exercises the fixture, validation guards, reporting, and current SDK
workflows without fetching baselines.

## Artifacts and interpretation

- `comparison.md`: source pins, package sizes, imports, first invocation/replay
  medians, checkpoint counts/bytes, serializer results, and correctness failures.
- `metrics.json`: all of the above plus samples, checkpoint traces, configuration,
  dependency versions, platform, and hashes of the benchmark sources.
- `current.json`, `v2.json`, `official.json`: completed per-SDK worker results.
- `setup.log` and per-SDK `.log` files: build and measurement diagnostics.

Imports measure the public modules needed to define a workflow and use its codec,
with warm filesystem caches and bytecode writes disabled. Peak RSS includes the
interpreter and dependencies; it is unavailable on platforms without `resource`.
Code-line counts exclude blank lines, comments, and standalone string-literal
documentation statements, including attribute docstrings, matching issue #323.
Class/function counts include private, nested, and method definitions. Package
sizes cover different feature scopes: this SDK bundles runner support, while
AWS distributes testing tools separately. The suite does not measure own-suite
coverage or public API documentation coverage.

These are local measurements, not Lambda performance predictions. The fixture
does not enforce service quotas or payload limits, exercise retries/suspension,
or model real I/O and worker saturation. Each SDK replays its own history; this
is not a history migration test. Request bytes are compact JSON of injected
client arguments, excluding HTTP headers and transport-added fields. Request
counts are not billing estimates. Use the full sampling mode on an otherwise
idle machine and compare exact pins before interpreting timing differences.

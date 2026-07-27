# Repository Instructions

Use the repository documentation as the source of truth. Do not duplicate SDK
guidance in this file.

## Read Before Editing

Start with the documents relevant to the task:

- `README.md` and `docs/index.md` for project scope and the execution model.
- `docs/getting-started.md` for replay rules and basic workflow structure.
- `docs/api/` and `docs/advanced-usage.md` for public APIs and composition.
- `docs/workflow-patterns.md` for agentic loops, approvals, and compensation.
- `docs/deployment.md` for IAM, qualified function names, invocation, and IaC.
- `docs/using-synchronous-code.md` for synchronous or blocking integrations.
- `docs/api/runner.md` and `docs/runner-architecture.md` for local and cloud
  runner behavior.
- `CONTRIBUTING.md` for repository layout, commands, tests, documentation,
  examples, and pull request expectations.
- `RELEASING.md` for release work.

Read nearby source, tests, examples, and docstrings before changing behavior.
For public API changes, also read the corresponding migration and SDK comparison
documents when they are affected.

## Working Expectations

- Preserve the replay contract described in `docs/index.md`: replayed workflow
  code is deterministic, and nondeterministic work or side effects are
  checkpointed by durable operations.
- Follow existing patterns and keep changes focused.
- Add or update tests for behavior changes. Use the matching test directory
  described in `CONTRIBUTING.md`.
- Update the authoritative document when behavior, public APIs, deployment
  requirements, or developer workflows change. Keep `AGENTS.md` as an index.
- Run the focused tests first, then the relevant formatting, type, and
  documentation checks from `CONTRIBUTING.md`.
- Do not edit generated output under `build/`, `site/`, or `dist/`.

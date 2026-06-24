# Contributing Guidelines

Thanks for your interest in contributing to `async-durable-execution`.
We welcome bug fixes, documentation improvements, examples, test coverage,
and new features that fit the goals of the project.

This guide focuses on the repository-specific workflow so it is easy to get
started without having to reverse-engineer the toolchain.

## Getting Started

Install [Hatch](https://hatch.pypa.io/dev/install/). All shared development
commands in this repository are run through Hatch from the repository root.

## Repository Structure

This repository is a monorepo with three Python packages at the repository root:

```text
async-durable-execution/           # Core SDK
async-durable-execution-runner/    # Local/cloud runner and pytest helpers
async-durable-execution-examples/  # Example functions and tests
```

The root `pyproject.toml` defines shared Hatch environments for testing,
typing, and examples. Each package-level `pyproject.toml` contains the
package metadata and package-local tool configuration.

## Development Workflow

Run commands from the repository root unless a command explicitly says
otherwise.

### Common commands

```bash
# Run all tests across all packages
hatch run test:all

# Run all tests with coverage
hatch run test:cov

# Type check the repo
hatch run types:check
```

### Focused package development

```bash
# Core SDK
hatch run dev-core:test
hatch run dev-core:cov
hatch run dev-core:typecheck

# Runner
hatch run dev-testing:test
hatch run dev-testing:cov
hatch run dev-testing:typecheck

# Examples
hatch run dev-examples:test
```

### Formatting and linting

Ruff configuration is package-local, so run formatting checks from the package
directory you are working in:

```bash
cd async-durable-execution
hatch fmt --check

# Or apply formatting fixes
hatch fmt
```

### Testing examples against PyPI

To verify the examples package against the published SDK while still using the
local runner package:

```bash
hatch run test-pypi-examples:test
```

## Coding Expectations

Please optimize for readability, maintainability, and consistency with the
existing codebase.

- Follow the established patterns in nearby code.
- Use Ruff and mypy feedback to guide formatting and type hints.
- Prefer small, focused pull requests over broad refactors.
- Keep runtime dependencies lightweight unless there is a strong reason to add
  one.
- Add or update tests when changing behavior.

Strong typing and dataclasses are used heavily across the project, but the goal
is clarity rather than rigid style for its own sake. Use the amount of typing
and structure that makes the code easier to understand and maintain.

## Writing and Running Tests

### Running tests

```bash
# Entire repo
hatch run test:all

# One package
hatch run dev-core:test

# A single test file
hatch run dev-core:test async-durable-execution/test_sdk/path_to_test_module.py

# A single test
hatch run dev-core:test async-durable-execution/test_sdk/path_to_test_module.py::test_name

# Filter by pattern
hatch run test:all -k pattern
```

### Debugging tests

```bash
hatch run test:all --pdb
```

### Test layout

- Put tests in the package `tests/` or `test/` directory that matches the code
  you are changing.
- Use filenames ending in `_test.py`.
- Prefer adding focused unit tests near the affected area, and add integration
  coverage when behavior spans multiple components.

## Examples and Deployment

Run example-related commands from the repository root:

```bash
# Run example tests
hatch run dev-examples:test

# Refresh editable installs in the examples environment when needed
hatch run -- examples:pip install -e async-durable-execution
hatch run -- examples:pip install -e async-durable-execution-runner
hatch run -- examples:pip install -e async-durable-execution-examples

# Build the shared example bundle
hatch run examples:build

# Generate a SAM template for all examples
hatch run examples:generate-sam-template

# Generate a SAM template for one example
hatch run examples:generate-sam-template -- --example-name "Hello World"

# Clean generated artifacts
hatch run examples:clean
```

## Pull Requests

Contributions through pull requests are appreciated.

Before opening a pull request:

1. Make sure your branch is based on the latest relevant source.
2. Check whether an issue or PR already covers the work.
3. For larger changes, open an issue or discussion first so we can align on
   scope before you invest a lot of time.
4. Run the relevant tests and checks locally.

When preparing a pull request:

1. Keep the change focused.
2. Explain the problem and the approach clearly.
3. Mention any follow-up work or known limitations.
4. Stay engaged with CI results and review feedback.

### Pull request titles and commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/) for pull
request titles and prefer the same style for commit messages:

```text
type: short description
```

Common types include:

- `feat`
- `fix`
- `docs`
- `test`
- `refactor`
- `perf`
- `style`
- `chore`
- `ci`
- `build`
- `deps`

Examples:

```text
feat: add retry support for callback polling
fix: preserve child context summary on replay
docs: clarify local runner setup
```

## Reporting Bugs and Requesting Features

GitHub issues are the best place to report bugs, request features, or suggest
documentation improvements. Before opening a new issue, please check for an
existing one first.

Helpful issue details include:

- a reproducible example or clear reproduction steps
- the package and version involved
- relevant logs or error messages
- environment details that might matter

## Code of Conduct

Please read [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community expectations.

## Security

If you discover a potential security issue, please do not open a public GitHub
issue. Contact the maintainers privately through an available non-public
channel.

## Licensing

See [LICENSE](LICENSE) for project licensing. By contributing, you agree that
your contributions may be distributed under the same license.

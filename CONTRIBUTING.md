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

The SDK, examples, and their tests all live at the repository root:

```text
async_durable_execution/  # Journal runtime and public SDK interfaces
tests/                    # SDK and runner tests
examples/                 # Example durable functions
test_examples/            # Local and cloud example tests
```

The root `pyproject.toml` contains the SDK package metadata and all Hatch,
pytest, coverage, mypy, and Ruff configuration.

## Development Workflow

Run commands from the repository root unless a command explicitly says
otherwise.

### Common commands

```bash
# Run all tests
hatch run test:all

# Run all tests with coverage
hatch run test:cov

# Type check the repo
hatch run test:typecheck
```

### Focused package development

```bash
# SDK and runner
hatch run test:sdk
hatch run test:runner

# Examples
hatch run test:examples
```

### Formatting and linting

Run formatting checks from the repository root:

```bash
hatch fmt --check

# Or apply formatting fixes
hatch fmt
```

### Documentation

Build the searchable documentation site and generated API reference:

```bash
hatch run docs:build
```

The build runs in strict mode and writes the site to `build/docs`. Fix warnings
about navigation, links, docstrings, or API signatures before submitting
documentation changes.

### Testing examples against PyPI

To verify the examples against the published SDK:

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

# SDK tests
hatch run test:sdk

# A single test file
hatch run test:sdk tests/path_to_test_module.py

# A single test
hatch run test:sdk tests/path_to_test_module.py::test_name

# Filter by pattern
hatch run test:all -k pattern
```

### Debugging tests

```bash
hatch run test:all --pdb
```

### Common troubleshooting

- `TimeoutError: Execution did not complete within 60s` - Increase the runner
  timeout, for example `timeout=120`.
- Import runner APIs from `async_durable_execution`. Removed private v2 package
  paths are not supported by the replacement.

### Test layout

- Put public API and observable workflow tests under `tests/`. The new suite
  groups workflow behavior, DAGs, serialization, transport boundaries, and the
  public API contract in separate files.
- Compatibility checks must not require private module paths, private class
  inheritance, or an implementation-specific executor hierarchy.
- Put consumer example tests under `test_examples/`; their `durable_runner`
  fixture selects the local or cloud public runner.
- Keep packaging and deployment-tool tests in `scripts/test_scripts/`.
- Cover replay, interruption, and cancellation when changing durable behavior.

## Example Integration Tests and Deployment

Run example-related commands from the repository root.

The examples include pytest coverage that can run against either the local
in-memory runner or deployed AWS Lambda durable functions. Local mode is the
default and does not require AWS credentials:

```bash
# Run all example tests locally.
hatch run test:examples

# Or run pytest directly with an explicit mode.
pytest --runner-mode=local test_examples/

# Run a specific example test.
pytest --runner-mode=local -k test_hello_world test_examples/
```

Refresh editable installs in the examples environment when needed:

```bash
hatch run -- examples:pip install -e .
```

Cloud mode exercises deployed Lambda functions with `DurableFunctionCloudTestRunner`:

```bash
# Build the SDK Lambda layer.
hatch run examples:build-layer

# Build the example bundle.
hatch run examples:build

# Generate a one-example SAM template.
hatch run examples:generate-sam-template -- --example-name "Hello World"

# Deploy the function with SAM.
sam build --template-file template.generated.json
sam deploy \
  --template-file .aws-sam/build/template.yaml \
  --stack-name hello-world-test \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --no-confirm-changeset \
  --parameter-overrides \
    FunctionNamePrefix=hello-world-test- \
    LambdaEndpoint=https://lambda.eu-south-1.amazonaws.com

# Configure cloud test discovery.
export AWS_REGION=eu-south-1
export LAMBDA_ENDPOINT=https://lambda.eu-south-1.amazonaws.com
export QUALIFIED_FUNCTION_NAME="hello-world-test-HelloWorld:$LATEST"

# Run one cloud-backed example test.
pytest --runner-mode=cloud -k test_hello_world test_examples/

# Or run via hatch.
hatch run test:examples-integration -k test_hello_world
```

For full-suite cloud runs where functions share a deployment prefix:

```bash
export PYTEST_FUNCTION_NAME_PREFIX="py313-"
hatch run test:examples-integration
```

Example tests use the `durable_runner` pytest fixture as a factory context manager:

```python
from async_durable_execution import InvocationStatus
from examples import hello_world


async def test_hello_world(durable_runner):
    with durable_runner(
        handler=hello_world.handler,
        input="test",
        timeout=30,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "statusCode": 200,
        "body": "Hello from Durable Lambda! (status: 200)",
    }
```

Cloud test configuration:

| Setting | Description |
| --- | --- |
| `AWS_REGION` | AWS region for Lambda invocation. Defaults to `eu-south-1`. |
| `LAMBDA_ENDPOINT` | Optional Lambda endpoint URL for testing. |
| `PYTEST_FUNCTION_NAME_PREFIX` | Prefix used to derive deployed qualified function names for all examples. |
| `QUALIFIED_FUNCTION_NAME` | Optional fallback for single-function cloud runs. |
| `FILESYSTEM_SERDES_CLOUD_MOUNT_PATH` | Optional durable EFS or S3 Files mount used by filesystem SerDes example tests. The deployed functions must have the mount configured. |
| `--runner-mode` | Pytest mode: `local` or `cloud`. |

Additional deployment helpers:

```bash
# Generate a SAM template for all examples
hatch run examples:generate-sam-template

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

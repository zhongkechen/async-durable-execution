# Async Durable Execution for Python

[![Build](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml)
[![API Docs](https://img.shields.io/badge/API%20Docs-GitHub%20Pages-0A7BBB)](https://zhongkechen.github.io/async-durable-execution/)
[![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![Runner PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution-runner.svg)](https://pypi.org/project/async-durable-execution-runner)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/zhongkechen/async-durable-execution/badge)](https://scorecard.dev/viewer/?uri=github.com/zhongkechen/async-durable-execution)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

-----

Build reliable, long-running AWS Lambda workflows with checkpointed steps, waits, callbacks, and parallel execution.

This repository is a community-maintained fork of the original Apache-2.0 licensed AWS project and continues to ship under Apache License 2.0 with the upstream notices preserved.

This fork is specifically focused on making async Python work naturally with durable functions. The public API remains synchronous at the durable operation boundary, but user-provided durable callables must now use `async def` for handlers, steps, child contexts, callback submitters, and condition checks.

## ✨ Key Features

- **Community-maintained fork of the official AWS SDK** - This repository builds on `aws/aws-durable-execution-sdk-python` and preserves the upstream Apache-2.0 notices while evolving the Python experience independently
- **Async-first user programming model** - Unlike the official library, this fork requires user-provided durable code to use `async def` for handlers, steps, child contexts, callback submitters, and wait-for-condition checks
- **Ergonomic async call-site helpers** - Use `@durable_callable` together with top-level awaitable operations like `step(...)`, `wait(...)`, and `run_in_child_context(...)` to keep durable workflow code explicit and natural in async Python
- **Same durable primitives, adapted for async Python** - Checkpointed steps, waits, callbacks, parallel branches, maps, retries, and child contexts are all preserved, but tuned for an async execution style
- **Replay-safe logging with stdlib logging** - Use standard `logging` loggers enriched by the durable context filter instead of relying on ad hoc logging patterns
- **Monorepo with SDK, runner, and examples together** - This fork ships the execution SDK, local/cloud runner, and example workflows in one repository so development and verification stay aligned
- **Stronger local and cloud validation workflow** - The repo includes a dedicated runner package, integration examples, and GitHub Actions automation for build, test, and generated API docs

## 📦 Packages

| Package | Description | Version |
| --- | --- | --- |
| `async-durable-execution` | Execution SDK for Lambda durable functions | [![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution) |
| `async-durable-execution-runner` | Local/cloud test runner and pytest helpers | [![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution-runner.svg)](https://pypi.org/project/async-durable-execution-runner) |
| `async-durable-execution-examples` | Example durable functions and integration tests for local and cloud workflows | Shared repo version |

## 🚀 Quick Start

This fork now requires async callables for all user-provided durable code.
Requires Python 3.10 or newer.

Install the execution SDK:

```console
pip install async-durable-execution
```

Create a durable Lambda handler:

```python
import asyncio
import logging
from datetime import timedelta

from async_durable_execution import (
    durable_callable,
    durable_execution,
    step,
    wait,
)

logger = logging.getLogger(__name__)


@durable_callable
async def validate_order(order_id: str) -> dict:
    await asyncio.sleep(0)
    logger.info("Validating order", extra={"order_id": order_id})
    return {"order_id": order_id, "valid": True}


@durable_execution
async def handler(event: dict) -> dict:
    order_id = event["order_id"]
    logger.info("Starting workflow", extra={"order_id": order_id})

    validation = await step(validate_order(order_id), name="validate_order")
    if not validation["valid"]:
        return {"status": "rejected", "order_id": order_id}

    # simulate approval (real world: use wait_for_callback)
    await wait(duration=timedelta(seconds=5), name="await_confirmation")

    return {"status": "approved", "order_id": order_id}
```

Async callables are required anywhere the SDK accepts user code, including `map()` item functions, bound `parallel()` branch callables, child contexts, callback submitters, and wait-for-condition checks. Durable context operations are awaitable and run on the same event loop as your handler:

```python
import asyncio
import logging

from async_durable_execution import (
    durable_callable,
    durable_execution,
    step,
)

logger = logging.getLogger(__name__)


@durable_callable
async def fetch_order(order_id: str) -> dict:
    await asyncio.sleep(0)
    logger.info("Fetched order", extra={"order_id": order_id})
    return {"order_id": order_id, "status": "ready"}


@durable_execution
async def handler(event: dict) -> dict:
    order = await step(fetch_order(event["order_id"]), name="fetch_order")
    return {"order": order}
```

Handler input is deserialized from the durable execution payload before your code runs. Empty or whitespace payloads are normalized to `{}`, and malformed JSON fails the invocation before user code executes.

## 🧪 Testing Durable Functions

Install the runner package to test durable functions locally or against deployed Lambda functions:

```console
pip install async-durable-execution-runner
```

The local runner executes the durable handler in process, intercepts checkpoint operations with an in-memory service client, and returns a `DurableFunctionTestResult` that can be inspected by operation name.

```python
import json
from datetime import timedelta
from functools import partial
from typing import Any

from async_durable_execution import (
    InvocationStatus,
    durable_execution,
    run_in_child_context,
    step,
    wait,
)
from async_durable_execution_runner import (
    ContextOperation,
    DurableFunctionLocalTestRunner,
    DurableFunctionTestResult,
    StepOperation,
)


async def one(a: int, b: int) -> str:
    return f"{a} {b}"


async def two_1(a: int, b: int) -> str:
    return f"{a} {b}"


async def two_2(a: int, b: int) -> str:
    return f"{b} {a}"


async def two(a: int, b: int) -> str:
    two_1_result = await step(partial(two_1, a, b))
    two_2_result = await step(partial(two_2, a, b))
    return f"{two_1_result} {two_2_result}"


async def three(a: int, b: int) -> str:
    return f"{a} {b}"


@durable_execution
async def function_under_test(event: Any) -> list[str]:
    results: list[str] = []

    result_one = await step(partial(one, 1, 2))
    results.append(result_one)

    await wait(timedelta(seconds=1))

    result_two = await run_in_child_context(partial(two, 3, 4), name="two")
    results.append(result_two)

    result_three = await step(partial(three, 5, 6))
    results.append(result_three)

    return results


async def test_my_durable_function() -> None:
    with DurableFunctionLocalTestRunner(
        handler=function_under_test,
        input="input str",
        timeout=10,
    ) as runner:
        result: DurableFunctionTestResult = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps(["1 2", "3 4 4 3", "5 6"])

    one_result: StepOperation = result.get_step("one")
    assert one_result.result == json.dumps("1 2")

    two_result: ContextOperation = result.get_context("two")
    assert two_result.result == json.dumps("3 4 4 3")
```

The `create_runner()` factory selects local or cloud mode from one call shape:

```python
from async_durable_execution_runner import create_runner


async def test_with_factory() -> None:
    with create_runner(
        mode="local",
        handler=function_under_test,
        input={"hello": "world"},
        timeout=12,
    ) as runner:
        local_result = await runner.run()

    with create_runner(
        mode="cloud",
        function_name="hello-world:$LATEST",
        region="us-east-1",
        input={"hello": "world"},
        timeout=45,
    ) as runner:
        cloud_result = await runner.run()
```

## 🧩 Example Integration Tests

The examples package includes pytest coverage that can run against either the local in-memory runner or deployed AWS Lambda durable functions.

Local mode is the default and does not require AWS credentials:

```console
# Run all example tests locally from the repo root.
hatch run dev-examples:test

# Or run pytest directly with an explicit mode.
pytest --runner-mode=local async-durable-execution-examples/test_examples/

# Run a specific example test.
pytest --runner-mode=local -k test_hello_world async-durable-execution-examples/test_examples/
```

Cloud mode exercises deployed Lambda functions with `DurableFunctionCloudTestRunner`:

```console
# Build the example bundle from the repo root.
hatch run examples:build

# Generate a one-example SAM template.
hatch run examples:generate-sam-template -- --example-name "Hello World"

# Deploy the function with SAM.
sam build --template-file async-durable-execution-examples/template.generated.json
sam deploy \
  --template-file .aws-sam/build/template.yaml \
  --stack-name hello-world-test \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --no-confirm-changeset \
  --parameter-overrides \
    FunctionName=HelloWorld-Test \
    LambdaEndpoint=https://lambda.eu-south-1.amazonaws.com

# Configure cloud test discovery.
export AWS_REGION=eu-south-1
export LAMBDA_ENDPOINT=https://lambda.eu-south-1.amazonaws.com
export QUALIFIED_FUNCTION_NAME="HelloWorld-Test:$LATEST"

# Run one cloud-backed example test.
pytest --runner-mode=cloud -k test_hello_world async-durable-execution-examples/test_examples/

# Or run via hatch.
hatch run test:examples-integration -k test_hello_world
```

For full-suite cloud runs where functions share a deployment prefix:

```console
export PYTEST_FUNCTION_NAME_PREFIX="py313-"
hatch run test:examples-integration
```

Example tests use the `durable_runner` pytest fixture as a factory context manager:

```python
from async_durable_execution import InvocationStatus
from async_durable_execution_examples import hello_world


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
| `--runner-mode` | Pytest mode: `local` or `cloud`. |

## 🏗️ Runner Architecture

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

- [Durable Functions Python Test Framework Architecture](async-durable-execution-runner/assets/dar-python-test-framework-architecture.svg)
- [Event Flow Sequence Diagram](async-durable-execution-runner/assets/dar-python-test-framework-event-flow.svg)

## 🛠️ Development

This repository uses Hatch workspaces to manage package environments and dependencies:

```console
# Run the full pytest suite.
hatch run test:all

# Run SDK tests.
hatch run dev-core:test

# Run runner tests.
hatch run dev-testing:test

# Run example tests locally.
hatch run dev-examples:test

# Run type checks.
hatch run types:check
```

CI also runs deployed example integration tests after generating and deploying SAM templates. See [.github/workflows/e2e-tests.yml](.github/workflows/e2e-tests.yml) for details.

Common troubleshooting notes:

- `TimeoutError: Execution did not complete within 60s` - Increase the runner timeout, for example `timeout=120`.
- `ModuleNotFoundError: No module named 'async_durable_execution_runner'` - Run through Hatch, such as `hatch run dev-examples:test`, so workspace dependencies are installed automatically.

## 📚 Documentation

The complete documentation for the AWS Durable Execution SDK for Python lives on the AWS Documentation site:

- **[Generated API Reference](https://zhongkechen.github.io/async-durable-execution/)** - Auto-generated from Python docstrings and published with GitHub Pages
- **[AWS Durable Execution Documentation](https://docs.aws.amazon.com/durable-execution/)** - Concepts, getting started, core operations, advanced topics, and API reference
- **[AWS Lambda Durable Functions Guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)** - How durable functions work on Lambda

## 💬 Feedback & Support

- [Bug report](https://github.com/zhongkechen/async-durable-execution/issues/new?template=bug_report.yml)
- [Feature request](https://github.com/zhongkechen/async-durable-execution/issues/new?template=feature_request.yml)
- [Documentation feedback](https://github.com/zhongkechen/async-durable-execution/issues/new?template=documentation.yml)
- [Contributing guide](CONTRIBUTING.md)

## 📄 License

See the [LICENSE](LICENSE) file for our project's licensing.

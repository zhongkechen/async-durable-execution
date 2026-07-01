# Async Durable Execution for Python

[![Build](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml)
[![API Docs](https://img.shields.io/badge/API%20Docs-GitHub%20Pages-0A7BBB)](https://zhongkechen.github.io/async-durable-execution/)
[![Coverage](https://zhongkechen.github.io/async-durable-execution/coverage/badge.svg)](https://zhongkechen.github.io/async-durable-execution/coverage/)
[![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
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
- **SDK and runner shipped together** - The execution SDK now includes the local/cloud runner through the `async_durable_execution` public API
- **Stronger local and cloud validation workflow** - The repo includes runner integration examples and GitHub Actions automation for build, test, and generated API docs

## 📦 Packages

| Package | Description | Version |
| --- | --- | --- |
| `async-durable-execution` | Execution SDK, local/cloud test runner, and pytest helpers for Lambda durable functions | [![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution) |
| `async-durable-execution-examples` | Example durable functions and integration tests for local and cloud workflows | Shared repo version |

Install `async-durable-execution` and import runner helpers from
`async_durable_execution`.

## 🚀 Quick Start

This fork now requires async callables for all user-provided durable code.
Requires Python 3.10 or newer.

Install the execution SDK:

```console
pip install async-durable-execution
```

For an async Lambda service client, install the optional `aioboto` extra:

```console
pip install "async-durable-execution[aioboto]"
```

The `aioboto` extra installs the published `aiobotocore` package. When it is
installed, the SDK creates an async Lambda client by default for durable
checkpoint and state APIs. Without the extra, it continues to use the bundled
`botocore` dependency through a threaded async adapter. Explicitly provided Lambda
API clients are detected as sync or async and wrapped accordingly. Code that must
force the sync `botocore` client can use
`async_durable_execution.client.create_default_sync_client()`.

To share the SDK through a Lambda layer instead of vendoring it in each function
zip, publish the repository-built layer from GitHub Actions or build a local
layer archive from this checkout:

```console
hatch run python scripts/build_layer.py \
  --sdk-source async-durable-execution \
  --output dist/async-durable-execution-layer.zip
```

Publish the zip as an `AWS::Serverless::LayerVersion` or
`AWS::Lambda::LayerVersion`, then add the layer ARN to Python durable functions.

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


@durable_callable
async def create_receipt(order_id: str) -> dict:
    await asyncio.sleep(0)
    logger.info("Creating receipt", extra={"order_id": order_id})
    return {"receipt_id": f"receipt-{order_id}", "order_id": order_id}


@durable_execution
async def handler(event: dict) -> dict:
    order_id = event["order_id"]
    logger.info("Starting workflow", extra={"order_id": order_id})

    validation = await step(validate_order(order_id), name="validate_order")
    if not validation["valid"]:
        return {"status": "rejected", "order_id": order_id}

    # simulate approval (real world: use wait_for_callback)
    await wait(duration=timedelta(seconds=5), name="await_confirmation")

    receipt = await step(create_receipt(order_id), name="create_receipt")

    return {"status": "approved", "order_id": order_id, "receipt": receipt}
```

Async callables are required anywhere the SDK accepts user code, including `map()` item functions, bound `parallel()` branch callables, child contexts, callback submitters, and wait-for-condition checks. Durable context operations are awaitable and run on the same event loop as your handler.

When decorating class or static methods, `@durable_callable` can be used in either order with `@classmethod` or `@staticmethod`; both of these are valid:

```python
class Steps:
    @durable_callable
    @classmethod
    async def from_class(cls) -> str:
        return cls.__name__

    @staticmethod
    @durable_callable
    async def from_static() -> str:
        return "ok"
```

Handler input is deserialized from the durable execution payload before your code runs. Empty or whitespace payloads are normalized to `{}`, and malformed JSON fails the invocation before user code executes.

## 🧪 Testing Durable Functions

The SDK includes runner helpers for testing durable functions locally or against deployed Lambda functions:

```console
pip install async-durable-execution
```

The local runner executes the durable handler in process, intercepts checkpoint operations with an in-memory service client, and returns a `DurableFunctionTestResult` that can be inspected by operation name.

Assuming the Quick Start handler above is saved in `order_workflow.py`, a local test can run the same durable function:

```python
import json

from async_durable_execution import (
    DurableFunctionTestResult,
    InvocationStatus,
    create_local_runner,
)

from order_workflow import handler


async def test_my_durable_function() -> None:
    with create_local_runner(
        handler=handler,
        input={"order_id": "order-123"},
        timeout=10,
    ) as runner:
        result: DurableFunctionTestResult = await runner.run()

    receipt = {"receipt_id": "receipt-order-123", "order_id": "order-123"}

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps(
        {"status": "approved", "order_id": "order-123", "receipt": receipt}
    )

    validation_result = result.get_step("validate_order")
    assert validation_result.step_details is not None
    assert validation_result.step_details.result == json.dumps(
        {"order_id": "order-123", "valid": True}
    )

    receipt_result = result.get_step("create_receipt")
    assert receipt_result.step_details is not None
    assert receipt_result.step_details.result == json.dumps(receipt)
```

Use the runner-specific factory for the environment you want to test:

```python
from async_durable_execution import create_cloud_runner, create_local_runner

from order_workflow import handler


async def test_with_factory() -> None:
    with create_local_runner(
        handler=handler,
        input={"order_id": "order-123"},
        timeout=12,
    ) as runner:
        local_result = await runner.run()

    with create_cloud_runner(
        function_name="order-workflow:$LATEST",
        region="us-east-1",
        input={"order_id": "order-123"},
        timeout=45,
    ) as runner:
        cloud_result = await runner.run()
```

## 🧩 Example Integration Tests

The examples package includes pytest coverage that can run against either the local in-memory runner or deployed AWS Lambda durable functions.

Local mode is the default and does not require AWS credentials:

```console
# Run all example tests locally from the repo root.
hatch run test:examples

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

## 📚 Documentation

The complete documentation for the AWS Durable Execution SDK for Python lives on the AWS Documentation site:

- **[Generated API Reference](https://zhongkechen.github.io/async-durable-execution/)** - Auto-generated from Python docstrings and published with GitHub Pages
- **[AWS Durable Execution Documentation](https://docs.aws.amazon.com/durable-execution/)** - Concepts, getting started, core operations, advanced topics, and API reference
- **[AWS Lambda Durable Functions Guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)** - How durable functions work on Lambda
- **[Migration Guide](docs/migrating-from-official-python-sdk.md)** - Move from the official synchronous Python SDK to this async-first SDK
- **[Using Synchronous Code](docs/using-synchronous-code.md)** - Wrap existing synchronous business logic and blocking clients safely
- **[Runner Architecture](docs/runner-architecture.md)** - Local and cloud runner execution flow, components, and diagrams
- **[Contributing Guide](CONTRIBUTING.md)** - Development workflow, Hatch commands, testing, and pull request guidance

## 💬 Feedback & Support

- [Bug report](https://github.com/zhongkechen/async-durable-execution/issues/new?template=bug_report.yml)
- [Feature request](https://github.com/zhongkechen/async-durable-execution/issues/new?template=feature_request.yml)
- [Documentation feedback](https://github.com/zhongkechen/async-durable-execution/issues/new?template=documentation.yml)
- [Contributing guide](CONTRIBUTING.md)

## 📄 License

See the [LICENSE](LICENSE) file for our project's licensing.

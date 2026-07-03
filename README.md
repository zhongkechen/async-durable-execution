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

- **Async-first durable code** - Compared with the official AWS SDK, user-provided durable handlers, steps, child contexts, callback submitters, map item functions, parallel branches, and wait-for-condition checks are written with `async def`.
- **Awaitable durable operations** - Workflow code now uses awaitable helpers such as `step(...)`, `wait(...)`, `invoke(...)`, `map(...)`, `parallel(...)`, and `run_in_child_context(...)`.
- **Simplified operation APIs** - The `v2` API removes config wrapper objects in favor of direct keyword arguments and clearer call sites, including keyword-only operation names.
- **Integrated local and cloud runner** - Runner functionality now ships through `async_durable_execution`, with separate local and cloud runner factories and typed test result helpers.
- **Async Lambda client support** - Install the optional `aioboto` extra to use an async Lambda client; otherwise the SDK uses the bundled sync client through an async adapter.
- **Replay-aware logging with stdlib logging** - Standard `logging` loggers are enriched by durable context filtering so workflow logs remain replay safe.
- **Lambda layer packaging** - The repo includes tooling and workflows to build and publish an SDK Lambda layer for functions that do not vendor dependencies directly.
- **Broader validation and docs** - The project now includes expanded local/cloud runner coverage, generated API docs, coverage publishing, and updated examples for async durable workflows.

## 🚀 Quick Start

Install the execution SDK:

```console
pip install async-durable-execution
```

For an async Lambda service client, install the optional `aioboto` extra:

```console
pip install "async-durable-execution[aioboto]"
```

The `aioboto` extra installs `aiobotocore`, which lets the SDK create an async Lambda client for durable checkpoint and state APIs. Without it, the SDK uses the bundled `botocore` dependency through a threaded async adapter.

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

Async callables are required anywhere the SDK accepts user code, including `map()` item functions, bound `parallel()` branch callables, child contexts, callback submitters, and wait-for-condition checks. Those callables can be functions, instance methods, class methods, or static methods. Durable context operations are awaitable and run on the same event loop as your handler.

Handler input is deserialized from the durable execution payload before your code runs. Empty or whitespace payloads are normalized to `{}`, and malformed JSON fails the invocation before user code executes.

## 🧪 Testing Durable Functions

The SDK includes runner helpers for testing durable functions locally or against deployed Lambda functions. The local runner executes the durable handler in process, intercepts checkpoint operations with an in-memory service client, and returns a `DurableFunctionTestResult` that can be inspected by operation name.

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

After deploying the same handler to Lambda, use the cloud runner to test the deployed durable function. The function name must be qualified with a version or alias, for example `order-workflow:$LATEST` or `order-workflow:prod`.

```python
import os

from async_durable_execution import InvocationStatus, create_cloud_runner


async def test_order_workflow_in_cloud() -> None:
    with create_cloud_runner(
        function_name=os.environ["ORDER_WORKFLOW_FUNCTION_NAME"],
        region=os.environ.get("AWS_REGION", "us-east-1"),
        input={"order_id": "order-123"},
        timeout=45,
    ) as runner:
        result = await runner.run()

    receipt = {"receipt_id": "receipt-order-123", "order_id": "order-123"}

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {
        "status": "approved",
        "order_id": "order-123",
        "receipt": receipt,
    }
```

## 🧩 Examples

Example durable functions live in `async-durable-execution-examples/src/async_durable_execution_examples/`. Start with `hello_world.py` for the smallest complete handler.

The example tests in `async-durable-execution-examples/test_examples/` are also useful as executable recipes. Browse them by operation or pattern:

- `step/`, `wait/`, `wait_for_callback/`, and `wait_for_condition/` for core durable operations
- `map/`, `parallel/`, and `run_in_child_context/` for composition patterns
- `invoke/`, `with_retry/`, `callback/`, and `logger_example/` for integrations and operational behavior

For the developer workflow to run or deploy example integration tests, see the [Contributing Guide](CONTRIBUTING.md#example-integration-tests-and-deployment).

## 📚 Documentation

- **[Generated API Reference](https://zhongkechen.github.io/async-durable-execution/)** - Auto-generated from Python docstrings and published with GitHub Pages
- **[Migration Guide](docs/migrating-from-official-python-sdk.md)** - Move from the official synchronous Python SDK to this async-first SDK
- **[Using Synchronous Code](docs/using-synchronous-code.md)** - Wrap existing synchronous business logic and blocking clients safely
- **[Advanced Usage](docs/advanced-usage.md)** - Configure Lambda clients and share the SDK through an AWS Lambda layer
- **[Runner Architecture](docs/runner-architecture.md)** - Local and cloud runner execution flow, components, and diagrams
- **[Contributing Guide](CONTRIBUTING.md)** - Development workflow, Hatch commands, testing, and pull request guidance

## References

- **[AWS Durable Execution Documentation](https://docs.aws.amazon.com/durable-execution/)** - Concepts, getting started, core operations, advanced topics, and API reference
- **[AWS Lambda Durable Functions Guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)** - How durable functions work on Lambda

## 💬 Feedback & Support

- [Bug report](https://github.com/zhongkechen/async-durable-execution/issues/new?template=bug_report.yml)
- [Feature request](https://github.com/zhongkechen/async-durable-execution/issues/new?template=feature_request.yml)
- [Documentation feedback](https://github.com/zhongkechen/async-durable-execution/issues/new?template=documentation.yml)
- [Contributing guide](CONTRIBUTING.md)

## 📄 License

See the [LICENSE](LICENSE) file for our project's licensing.

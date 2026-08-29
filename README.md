# Async Durable Execution for Python

[![简体中文](https://img.shields.io/badge/Language-%E7%AE%80%E4%BD%93%E4%B8%AD%E6%96%87-555555)](https://github.com/zhongkechen/async-durable-execution/blob/main/README.zh-CN.md)
[![繁體中文](https://img.shields.io/badge/Language-%E7%B9%81%E9%AB%94%E4%B8%AD%E6%96%87-555555)](https://github.com/zhongkechen/async-durable-execution/blob/main/README.zh-TW.md)
[![Quick start](https://img.shields.io/badge/Quick_start-Python-3776AB?logo=python&logoColor=white)](#quick-start)
[![Read the docs](https://img.shields.io/badge/Read_the_docs-API_reference-0A7BBB)](https://zhongkechen.github.io/async-durable-execution/)

[![Build](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml)
[![Conformance](https://github.com/zhongkechen/async-durable-execution/actions/workflows/conformance-tests.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/conformance-tests.yml)
[![Coverage](https://zhongkechen.github.io/async-durable-execution/coverage/badge.svg)](https://zhongkechen.github.io/async-durable-execution/coverage/)
[![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/zhongkechen/async-durable-execution/blob/main/LICENSE)

**Build fully compliant, long-running AWS Lambda workflows with native
`async`/`await`.** Checkpoint state automatically, pause without active compute,
and resume after failures without running a workflow server.

## Project Status

**Fully compliant with the
[AWS Durable Execution conformance suite](https://github.com/aws/aws-durable-execution-conformance-tests).**
Every upstream requirement is continuously validated against deployed Lambda
functions in CI.

The project also maintains extensive local and cloud runner coverage, publishes
generated [API documentation](https://zhongkechen.github.io/async-durable-execution/)
and [coverage reports](https://zhongkechen.github.io/async-durable-execution/coverage/),
and includes executable examples for async durable workflows.

> Community-maintained async fork of the Apache-2.0 licensed
> [AWS Durable Execution Python SDK](https://pypi.org/project/aws-durable-execution-sdk-python/).

This project continues to ship under Apache License 2.0 with the upstream
notices preserved.

The fork exists because the official Python SDK does not support
`async`/`await`, making integration with `asyncio` libraries difficult. This SDK
adds async durable callables, background operation tasks, direct `asyncio` task
composition, and APIs designed for modern Python applications.

## ✨ Key Features

- **[Async-first durable code](https://zhongkechen.github.io/async-durable-execution/official-python-sdk-comparison.html#programming-model)** - Compared with the official AWS SDK, user-provided durable handlers, steps, child contexts, `flow` nodes, callback submitters, map item functions, parallel branches, and wait-for-condition checks are written with `async def`.
- **[Operations not available in the official SDK](https://zhongkechen.github.io/async-durable-execution/api/extension/replay_safe.html)** - This SDK adds [replay-safe helpers](https://zhongkechen.github.io/async-durable-execution/api/extension/replay_safe.html) (`random()`, `now()`, `timestamp()`, and `uuid()`) and [durable self-invocation](https://zhongkechen.github.io/async-durable-execution/api/extension/recurse.html) (`recurse()`).
- **[Stable custom operation SPI](https://zhongkechen.github.io/async-durable-execution/custom-operations.html)** - Third-party packages can reserve opaque deterministic primitive identities, use custom subtypes, and build stateful replay-safe operations without importing SDK internals.
- **[Declarative DAG workflows](https://zhongkechen.github.io/async-durable-execution/api/extension/flow.html#quick-start)** - Define acyclic workflows with typed node inputs, inferred or conditional dependencies, failure routes, and durable operations inside each node. The SDK validates the graph before execution and skips nodes that are not required by the selected outputs.
- **[Background operation tasks](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#background-operation-tasks)** - Durable operations such as `step(...)`, `wait(...)`, `invoke(...)`, `recurse(...)`, `run_in_child_context(...)`, and `flow(...)` return `asyncio.Task` objects, so independent operations can run in the background and be awaited together with `asyncio.gather` without using `parallel()` or `map()`.
- **[Pythonic operation parameters](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html#api-mapping)** - Operations use direct keyword arguments, standard Python types such as `datetime.timedelta`, and keyword-only names instead of configuration wrapper objects.
- **[Composable SerDes pipelines](https://zhongkechen.github.io/async-durable-execution/serdes-pipelines.html)** - Chain async string transformations and offload large checkpoint payloads to EFS or S3 Files with bounded previews and validated content-addressed storage.
- **[Integrated local and cloud runner](https://zhongkechen.github.io/async-durable-execution/api/runner.html)** - Runner functionality now ships through `async_durable_execution`, with separate local and cloud runner factories and typed test result helpers.
- **[Model-free Lambda clients](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#lambda-client-selection)** - The SDK owns its Lambda REST wire format instead of depending on botocore service models. Install the optional `httpx` extra to send requests with HTTPX; otherwise the same requests use botocore's synchronous HTTP transport through an async adapter.
- **[Replay-aware logging with stdlib logging](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html#logging)** - Standard `logging` loggers are enriched by durable context filtering so workflow logs remain replay safe.
- **[Lambda layer packaging](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#lambda-layer-packaging)** - The repo includes tooling and workflows to build and publish an SDK Lambda layer for functions that do not vendor dependencies directly.

## 🚀 Quick Start

Install the execution SDK:

```console
pip install async-durable-execution
```

For an async Lambda service client, install the optional `httpx` extra:

```console
pip install "async-durable-execution[httpx]"
```

The `httpx` extra installs HTTPX, which the SDK uses for asynchronous
model-free Lambda REST calls. Without it, the SDK sends the same signed
requests with botocore's synchronous HTTP transport through a threaded async
adapter. Botocore continues to provide AWS credentials, endpoint metadata, and
SigV4 signing, but its generated Lambda service model is not used.

The previous `aioboto` extra remains available as a backward-compatible alias
for `httpx`.

Create a durable Lambda handler:

```python
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
    logger.info("Validating order", extra={"order_id": order_id})
    return {"order_id": order_id, "valid": True}


@durable_callable
async def create_receipt(order_id: str) -> dict:
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

Durable operations return `asyncio.Task` objects. If you call an operation without immediately awaiting it, it is scheduled to run in the background and can be awaited later. This lets independent operations run concurrently with normal `asyncio` patterns:

```python
import asyncio

pricing_tasks = [
    step(price_line_item(item), name=f"price-{item['sku']}")
    for item in items
]
priced_items = await asyncio.gather(*pricing_tasks)
```

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

Example durable functions live in `examples/`. Start with `hello_world.py` for the smallest complete handler.

The example tests in `test_examples/` are also useful as executable recipes. Browse them by operation or pattern:

- `step/`, `wait/`, `wait_for_callback/`, and `wait_for_condition/` for core durable operations
- `step/steps_with_gather.py` for starting multiple step tasks and awaiting them together with `asyncio.gather`
- `flow/`, `map/`, `parallel/`, and `run_in_child_context/` for composition patterns
- `invoke/`, including `invoke/recurse.py`, `with_retry/`, `callback/`, and `logger_example/` for integrations and operational behavior

For the developer workflow to run or deploy example integration tests, see the [Contributing Guide](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md#example-integration-tests-and-deployment).

## 📚 Documentation

- **[Documentation Site](https://zhongkechen.github.io/async-durable-execution/)** - Searchable guides and API reference generated from Python docstrings
- **[DAG Workflow API](https://zhongkechen.github.io/async-durable-execution/api/extension/flow.html)** - Build declarative workflows with `flow()`, typed node inputs, conditional dependencies, and failure routes
- **[Official Python SDK Comparison](https://zhongkechen.github.io/async-durable-execution/official-python-sdk-comparison.html)** - Side-by-side comparison with the official AWS Durable Execution Python SDK
- **[Migration Guide](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html)** - Move from the official synchronous Python SDK to this async-first SDK
- **[Workflow Patterns](https://zhongkechen.github.io/async-durable-execution/workflow-patterns.html)** - Build agentic loops, human approval workflows, and compensating transactions
- **[Deploy and Invoke](https://zhongkechen.github.io/async-durable-execution/deployment.html)** - Configure IAM, qualified function identifiers, invocations, CloudFormation, and SAM
- **[Using Synchronous Code](https://zhongkechen.github.io/async-durable-execution/using-synchronous-code.html)** - Wrap existing synchronous business logic and blocking clients safely
- **[Advanced Usage](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html)** - Explore background operation tasks, batch completion conditions, Lambda clients, and Lambda layers
- **[Custom Durable Operations](https://zhongkechen.github.io/async-durable-execution/custom-operations.html)** - Build third-party durable operation libraries on the stable extension-author interface
- **[Runner Architecture](https://zhongkechen.github.io/async-durable-execution/runner-architecture.html)** - Local and cloud runner execution flow, components, and diagrams
- **[Contributing Guide](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md)** - Development workflow, Hatch commands, testing, and pull request guidance

## References

- **[AWS Durable Execution Documentation](https://docs.aws.amazon.com/durable-execution/)** - Concepts, getting started, core operations, advanced topics, and API reference
- **[AWS Lambda Durable Functions Guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)** - How durable functions work on Lambda

## 💬 Feedback & Support

- [Bug report](https://github.com/zhongkechen/async-durable-execution/issues/new?template=bug_report.yml)
- [Feature request](https://github.com/zhongkechen/async-durable-execution/issues/new?template=feature_request.yml)
- [Documentation feedback](https://github.com/zhongkechen/async-durable-execution/issues/new?template=documentation.yml)
- [Contributing guide](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md)

## 📄 License

See the [LICENSE](https://github.com/zhongkechen/async-durable-execution/blob/main/LICENSE) file for our project's licensing.

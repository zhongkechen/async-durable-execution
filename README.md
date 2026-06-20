# Async Durable Execution for Python

[![Build](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml)
[![API Docs](https://img.shields.io/badge/API%20Docs-GitHub%20Pages-0A7BBB)](https://zhongkechen.github.io/async-durable-execution/)
[![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
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
    LambdaContext,
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
async def handler(event: dict, context: LambdaContext) -> dict:
    order_id = event["order_id"]
    logger.info("Starting workflow", extra={"order_id": order_id})

    validation = await step(validate_order(order_id), name="validate_order")
    if not validation["valid"]:
        return {"status": "rejected", "order_id": order_id}

    # simulate approval (real world: use wait_for_callback)
    await wait(duration=timedelta(seconds=5), name="await_confirmation")

    return {"status": "approved", "order_id": order_id}
```

Async callables are required anywhere the SDK accepts user code, including `map()` item functions, `parallel()` branches, child contexts, callback submitters, and wait-for-condition checks. Durable context operations are awaitable and run on the same event loop as your handler:

```python
import asyncio
import logging

from async_durable_execution import (
    LambdaContext,
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
async def handler(event: dict, context: LambdaContext) -> dict:
    order = await step(fetch_order(event["order_id"]), name="fetch_order")
    return {"order": order}
```

## 📚 Documentation

The complete documentation for the AWS Durable Execution SDK for Python lives on the AWS Documentation site:

- **[Generated API Reference](https://zhongkechen.github.io/async-durable-execution/)** - Auto-generated from Python docstrings and published with GitHub Pages
- **[AWS Lambda Durable Functions Guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)** - How durable functions work on Lambda

## 💬 Feedback & Support

- [Bug report](https://github.com/zhongkechen/async-durable-execution/issues/new?template=bug_report.yml)
- [Feature request](https://github.com/zhongkechen/async-durable-execution/issues/new?template=feature_request.yml)
- [Documentation feedback](https://github.com/zhongkechen/async-durable-execution/issues/new?template=documentation.yml)
- [Contributing guide](CONTRIBUTING.md)

## 📄 License

See the [LICENSE](LICENSE) file for our project's licensing.

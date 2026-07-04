# Python 异步持久执行

[English](README.md) | [繁體中文](README.zh-TW.md)

[![Build](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml)
[![API Docs](https://img.shields.io/badge/API%20Docs-GitHub%20Pages-0A7BBB)](https://zhongkechen.github.io/async-durable-execution/)
[![Coverage](https://zhongkechen.github.io/async-durable-execution/coverage/badge.svg)](https://zhongkechen.github.io/async-durable-execution/coverage/)
[![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/zhongkechen/async-durable-execution/badge)](https://scorecard.dev/viewer/?uri=github.com/zhongkechen/async-durable-execution)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

-----

使用带检查点的步骤 (`step`)、等待 (`wait`)、回调与并行执行，构建可靠、长时间运行的 Lambda 持久性函数。

本仓库是原 Apache-2.0 授权 [AWS Durable Execution Python SDK](https://pypi.org/project/aws-durable-execution-sdk-python/) 的社区维护分支，并在保留上游声明的同时，继续以 Apache License 2.0 发布。

创建此分支首先是因为官方 Python SDK 不支持 `async`/`await`，导致它很难与其他 `asyncio` 库良好协作。此分支专注于让异步 Python 更自然地搭配 Lambda 持久性函数使用。其次，官方 Python SDK 也缺少其他官方 SDK 已支持的能力，例如持久操作的后台执行和并行执行。这个 SDK 解决了这些问题，公开 API 在持久操作边界仍保持同步接口，用户提供的持久可调用对象必须对事件处理程序、步骤、子上下文、回调提交器与条件检查使用 `async def`，并采用了更加 Pythonic 的风格，让 API 在现代 Python 代码中使用起来更自然。

## ✨ 主要功能

- **异步优先的持久代码** - 与官方 AWS SDK 相比，用户提供的持久事件处理程序、步骤、子上下文、回调提交器、`map()` 项函数、`parallel()` 分支与等待条件检查都使用 `async def` 编写。
- **可 await 的持久操作** - 工作流程代码现在使用可 await 的辅助函数，例如 `step(...)`、`wait(...)`、`invoke(...)`、`map(...)`、`parallel(...)` 与 `run_in_child_context(...)`。
- **简化的持久操作 API** - `v2` API 移除了配置包装对象，改用直接的关键字参数与更清晰的调用位置，包括仅限关键字的操作名称。
- **集成本地与云端运行器** - 运行器功能现在通过 `async_durable_execution` 提供，包含独立的本地与云端运行器工厂，以及带类型的测试结果辅助对象。
- **支持异步 Lambda 客户端** - 安装可选的 `aioboto` extra 即可使用异步 Lambda 客户端；否则 SDK 会通过异步适配器使用内置的同步客户端。
- **通过标准库 logging 提供重放感知日志** - 标准 `logging` logger 会由持久上下文过滤器增强，让工作流程日志在重放时保持安全。
- **Lambda 层打包** - 仓库包含构建与发布 SDK Lambda 层的工具和工作流程，适用于不直接打包依赖项的函数。
- **更完整的验证与文档** - 项目现在包含扩展后的本地/云端运行器覆盖、生成的 API 文档、覆盖率发布，以及针对异步 Lambda 持久性函数更新的示例。

## 🚀 快速开始

安装 SDK：

```console
pip install async-durable-execution
```

如需异步 Lambda 服务客户端，请安装可选的 `aioboto` extra：

```console
pip install "async-durable-execution[aioboto]"
```

`aioboto` extra 会安装 `aiobotocore`，让 SDK 能为持久检查点与状态 API 创建异步 Lambda 客户端。若未安装，SDK 会通过线程化的异步适配器使用内置的 `botocore` 依赖项。

创建 Lambda 持久性函数的事件处理程序：

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

    # 模拟审批（实际场景请使用 wait_for_callback）
    await wait(duration=timedelta(seconds=5), name="await_confirmation")

    receipt = await step(create_receipt(order_id), name="create_receipt")

    return {"status": "approved", "order_id": order_id, "receipt": receipt}
```

SDK 接受用户代码的所有位置都必须使用异步可调用对象，包括 `map()` 项函数、绑定的 `parallel()` 分支可调用对象、子上下文、回调提交器与等待条件检查。这些可调用对象可以是函数、实例方法、类方法或静态方法。持久上下文操作是可 await 的，并与事件处理程序运行在同一个 event loop 上。

事件处理程序输入会在你的代码运行前，先从持久执行有效载荷反序列化。空白或仅包含空白字符的有效载荷会规范化为 `{}`，格式错误的 JSON 则会在用户代码运行前让调用失败。

## 🧪 测试 Lambda 持久性函数

SDK 包含运行器辅助函数，可用于在本地测试 Lambda 持久性函数，或针对已部署的 Lambda 函数进行测试。本地运行器会在进程内运行事件处理程序，使用内存内服务客户端拦截检查点操作，并返回可按操作名称检查的 `DurableFunctionTestResult`。

假设上方快速开始的事件处理程序保存在 `order_workflow.py`，本地测试可以运行同一个 Lambda 持久性函数：

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

将同一个事件处理程序部署到 Lambda 后，请使用云端运行器测试已部署的 Lambda 持久性函数。函数名称必须以版本或别名限定，例如 `order-workflow:$LATEST` 或 `order-workflow:prod`。

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

## 🧩 示例

Lambda 持久性函数示例位于 `async-durable-execution-examples/src/async_durable_execution_examples/`。可以从 `hello_world.py` 开始，它是最小的完整事件处理程序。

`async-durable-execution-examples/test_examples/` 中的示例测试也很适合作为可执行的模板参考。可按操作或模式浏览：

- `step/`、`wait/`、`wait_for_callback/` 与 `wait_for_condition/` 用于核心持久操作
- `map/`、`parallel/` 与 `run_in_child_context/` 用于组合模式
- `invoke/`、`with_retry/`、`callback/` 与 `logger_example/` 用于集成与运行行为

如需了解运行或部署示例集成测试的开发者工作流，请参阅[贡献指南](CONTRIBUTING.md#example-integration-tests-and-deployment)。

## 📚 文档

- **[生成的 API 参考](https://zhongkechen.github.io/async-durable-execution/)** - 从 Python docstring 自动生成，并通过 GitHub Pages 发布
- **[迁移指南](docs/migrating-from-official-python-sdk.md)** - 从官方同步 Python SDK 迁移到这个异步优先 SDK
- **[使用同步代码](docs/using-synchronous-code.md)** - 安全包装既有同步业务逻辑与阻塞式客户端
- **[高级用法](docs/advanced-usage.md)** - 配置 Lambda 客户端，并通过 AWS Lambda 层共享 SDK
- **[运行器架构](docs/runner-architecture.md)** - 本地与云端运行器的执行流程、组件与图表
- **[贡献指南](CONTRIBUTING.md)** - 开发工作流、Hatch 命令、测试与 pull request 指南

## 参考资料

- **[AWS 耐用执行 SDK 开发人员指南](https://docs.aws.amazon.com/durable-execution/)** - 概念、入门、核心操作、高级主题与 API 参考
- **[Lambda 持久性函数指南](https://docs.aws.amazon.com/zh_cn/lambda/latest/dg/durable-functions.html)** - Lambda 持久性函数的工作方式

## 💬 反馈与支持

- [Bug 报告](https://github.com/zhongkechen/async-durable-execution/issues/new?template=bug_report.yml)
- [功能请求](https://github.com/zhongkechen/async-durable-execution/issues/new?template=feature_request.yml)
- [文档反馈](https://github.com/zhongkechen/async-durable-execution/issues/new?template=documentation.yml)
- [贡献指南](CONTRIBUTING.md)

## 📄 许可证

请参阅 [LICENSE](LICENSE) 文件以了解本项目的许可信息。

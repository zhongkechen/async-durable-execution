# Python 异步持久执行

[![English](https://img.shields.io/badge/Language-English-555555)](README.md)
[![繁體中文](https://img.shields.io/badge/Language-%E7%B9%81%E9%AB%94%E4%B8%AD%E6%96%87-555555)](README.zh-TW.md)
[![Quick start](https://img.shields.io/badge/Quick_start-Python-3776AB?logo=python&logoColor=white)](#quick-start)
[![Read the docs](https://img.shields.io/badge/Read_the_docs-API_reference-0A7BBB)](https://zhongkechen.github.io/async-durable-execution/)

[![Build](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml)
[![Conformance](https://github.com/zhongkechen/async-durable-execution/actions/workflows/conformance-tests.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/conformance-tests.yml)
[![Coverage](https://zhongkechen.github.io/async-durable-execution/coverage/badge.svg)](https://zhongkechen.github.io/async-durable-execution/coverage/)
[![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**使用原生 `async`/`await` 构建完全符合 AWS Durable Execution 规范且可长时间运行的
AWS Lambda 工作流程。** 自动为状态创建检查点，无需持续计算即可暂停，并在故障后恢复执行，
无需运行工作流程服务器。

## 项目状态

**完全符合
[AWS Durable Execution 一致性测试套件](https://github.com/aws/aws-durable-execution-conformance-tests)。**
所有上游要求都会在 CI 中针对已部署的 Lambda 函数持续验证。

> 这是采用 Apache-2.0 许可证的
> [AWS Durable Execution Python SDK](https://pypi.org/project/aws-durable-execution-sdk-python/)
> 的社区维护异步分支。

本项目在保留上游声明的同时继续以 Apache License 2.0 发布。

创建此分支是因为官方 Python SDK 不支持 `async`/`await`，导致它难以与 `asyncio` 库集成。此 SDK 增加了异步持久可调用对象、后台操作任务、直接的 `asyncio` 任务组合，以及面向现代 Python 应用程序设计的 API。

## ✨ 主要功能

- **[异步优先的持久代码](https://zhongkechen.github.io/async-durable-execution/official-python-sdk-comparison.html#programming-model)** - 与官方 AWS SDK 相比，用户提供的持久事件处理程序、步骤、子上下文、`flow` 节点、回调提交器、`map()` 项函数、`parallel()` 分支与等待条件检查都使用 `async def` 编写。
- **[官方 SDK 未提供的扩展操作](https://zhongkechen.github.io/async-durable-execution/api/operations.html#sdk-extensions)** - 本 SDK 新增[重放安全辅助操作](https://zhongkechen.github.io/async-durable-execution/api/operations.html#replay-safe-helper-values)（`random()`、`now()`、`timestamp()` 与 `uuid()`）、[持久自调用](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#recursive-self-invocation)（`recurse()`），以及[声明式 DAG 执行](https://zhongkechen.github.io/async-durable-execution/api/operations.html#declarative-dag-workflows)（`flow()`）。
- **[声明式 DAG 工作流](https://zhongkechen.github.io/async-durable-execution/api/dag.html#quick-start)** - 使用带类型的节点输入、推导或条件依赖、失败路由和节点内持久操作来定义无环工作流。SDK 会在执行前验证图，并跳过所选输出不依赖的节点。
- **[后台操作任务](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#background-operation-tasks)** - `step(...)`、`wait(...)`、`invoke(...)`、`recurse(...)`、`run_in_child_context(...)` 与 `flow(...)` 等持久操作会返回 `asyncio.Task` 对象，因此独立操作可以在后台运行，并通过 `asyncio.gather` 一起等待，无需使用 `parallel()` 或 `map()`。
- **[简化的持久操作 API](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html#api-mapping)** - `v2` API 移除了配置包装对象，改用直接的关键字参数与更清晰的调用位置，包括仅限关键字的操作名称。
- **[集成本地与云端运行器](https://zhongkechen.github.io/async-durable-execution/async_durable_execution/runner.html#local-and-cloud-runners)** - 运行器功能现在通过 `async_durable_execution` 提供，包含独立的本地与云端运行器工厂，以及带类型的测试结果辅助对象。
- **[支持异步 Lambda 客户端](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#lambda-client-selection)** - 安装可选的 `aioboto` extra 即可使用异步 Lambda 客户端；否则 SDK 会通过异步适配器使用内置的同步客户端。
- **[通过标准库 logging 提供重放感知日志](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html#logging)** - 标准 `logging` logger 会由持久上下文过滤器增强，让工作流程日志在重放时保持安全。
- **[Lambda 层打包](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#lambda-layer-packaging)** - 仓库包含构建与发布 SDK Lambda 层的工具和工作流程，适用于不直接打包依赖项的函数。
- **[更完整的验证与文档](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md#development-workflow)** - 项目现在包含扩展后的本地/云端运行器覆盖、生成的 API 文档、覆盖率发布，以及针对异步 Lambda 持久性函数更新的示例。

<a id="quick-start"></a>

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

提供给 SDK 的工作流主体必须是异步的，包括持久事件处理程序、步骤可调用对象、`flow` 节点体、`map()` 项函数、绑定的 `parallel()` 分支可调用对象、子上下文、回调提交器与等待条件检查。这些可调用对象可以是函数、实例方法、类方法或静态方法。持久上下文操作是可 await 的，并与事件处理程序运行在同一个 event loop 上。

声明式定义和配置钩子则使用同步可调用对象，包括 `@durable_dag` 定义、重试与轮询策略、自定义完成回调、项命名器和摘要生成器。不要使用 `async def` 定义这些钩子；DAG 定义以及控制重放期间工作流结构或元数据的其他钩子必须保持确定性。

持久操作会返回 `asyncio.Task` 对象。如果调用操作后没有立即等待它，该操作会被安排在后台运行，之后仍可等待其结果。这让独立操作可以通过常规 `asyncio` 模式并发执行：

```python
pricing_tasks = [
    step(price_line_item(item), name=f"price-{item['sku']}")
    for item in items
]
priced_items = await asyncio.gather(*pricing_tasks)
```

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

Lambda 持久性函数示例位于 `examples/`。可以从 `hello_world.py` 开始，它是最小的完整事件处理程序。

`test_examples/` 中的示例测试也很适合作为可执行的模板参考。可按操作或模式浏览：

- `step/`、`wait/`、`wait_for_callback/` 与 `wait_for_condition/` 用于核心持久操作
- `step/steps_with_gather.py` 展示如何启动多个步骤任务，并通过 `asyncio.gather` 一起等待
- `flow/`、`map/`、`parallel/` 与 `run_in_child_context/` 用于组合模式
- `invoke/`（包括 `invoke/recurse.py`）、`with_retry/`、`callback/` 与 `logger_example/` 用于集成与运行行为

如需了解运行或部署示例集成测试的开发者工作流，请参阅[贡献指南](CONTRIBUTING.md#example-integration-tests-and-deployment)。

## 📚 文档

- **[文档网站](https://zhongkechen.github.io/async-durable-execution/)** - 可搜索的指南与从 Python docstring 生成的 API 参考
- **[DAG 工作流 API](docs/api/dag.md)** - 使用 `flow()`、带类型的节点输入、条件依赖和失败路由构建声明式工作流
- **[官方 Python SDK 对比](docs/official-python-sdk-comparison.md)** - 与官方 AWS Durable Execution Python SDK 的并排对比
- **[迁移指南](docs/migrating-from-official-python-sdk.md)** - 从官方同步 Python SDK 迁移到这个异步优先 SDK
- **[使用同步代码](docs/using-synchronous-code.md)** - 安全包装既有同步业务逻辑与阻塞式客户端
- **[高级用法](docs/advanced-usage.md)** - 了解后台操作任务、批量完成条件、Lambda 客户端与 Lambda 层
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

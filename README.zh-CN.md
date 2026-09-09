# Python 异步持久执行

> **V3 开发分支。** 这是保留 v2 顶层公共 API 的全新实现。
> 已完成的检查与发布要求请参阅[重写与验证状态](docs/v3-rewrite.md)。

[![English](https://img.shields.io/badge/Language-English-555555)](https://github.com/zhongkechen/async-durable-execution/blob/main/README.md)
[![繁體中文](https://img.shields.io/badge/Language-%E7%B9%81%E9%AB%94%E4%B8%AD%E6%96%87-555555)](https://github.com/zhongkechen/async-durable-execution/blob/main/README.zh-TW.md)
[![Quick start](https://img.shields.io/badge/Quick_start-Python-3776AB?logo=python&logoColor=white)](#quick-start)
[![Read the docs](https://img.shields.io/badge/Read_the_docs-API_reference-0A7BBB)](https://zhongkechen.github.io/async-durable-execution/)

[![Build](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml)
[![Conformance](https://github.com/zhongkechen/async-durable-execution/actions/workflows/conformance-tests.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/conformance-tests.yml)
[![Coverage](https://zhongkechen.github.io/async-durable-execution/coverage/badge.svg)](https://zhongkechen.github.io/async-durable-execution/coverage/)
[![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/zhongkechen/async-durable-execution/blob/main/LICENSE)

**使用原生 `async`/`await` 构建可长时间运行的
AWS Lambda 工作流程。** 自动为状态创建检查点，无需持续计算即可暂停，并在故障后恢复执行，
无需运行工作流程服务器。

## 项目状态

**V3 保留 v2 顶层公共 API。** 新的执行日志运行时使用已记录的公共 API 契约、
重新编写的行为测试与应用示例进行验证。私有模块导入和 v2 执行历史不予保留。

[AWS Durable Execution 一致性测试套件](https://github.com/aws/aws-durable-execution-conformance-tests)
仍是针对已部署 Lambda 函数的发布检查。本开发分支尚未完成该云端验证。

项目还维护完整的本地与云端运行器测试覆盖，发布生成的
[API 文档](https://zhongkechen.github.io/async-durable-execution/)与
[覆盖率报告](https://zhongkechen.github.io/async-durable-execution/coverage/)，
并提供异步持久工作流的可执行示例。

> 这是采用 Apache-2.0 许可证的
> [AWS Durable Execution Python SDK](https://pypi.org/project/aws-durable-execution-sdk-python/)
> 的社区维护异步分支。

本项目在保留上游声明的同时继续以 Apache License 2.0 发布。

创建此分支是因为官方 Python SDK 不支持 `async`/`await`，导致它难以与 `asyncio` 库集成。此 SDK 增加了异步持久可调用对象、后台操作任务、直接的 `asyncio` 任务组合，以及面向现代 Python 应用程序设计的 API。

## ✨ 主要功能

- **[异步优先的持久代码](https://zhongkechen.github.io/async-durable-execution/official-python-sdk-comparison.html#programming-model)** - 与官方 AWS SDK 相比，用户提供的持久事件处理程序、步骤、子上下文、`flow` 节点、回调提交器、`map()` 项函数、`parallel()` 分支与等待条件检查都使用 `async def` 编写。
- **[官方 SDK 未提供的扩展操作](https://zhongkechen.github.io/async-durable-execution/api/extension/replay_safe.html)** - 本 SDK 新增[重放安全辅助操作](https://zhongkechen.github.io/async-durable-execution/api/extension/replay_safe.html)（`random()`、`now()`、`timestamp()` 与 `uuid()`）和[持久自调用](https://zhongkechen.github.io/async-durable-execution/api/extension/recurse.html)（`recurse()`）。
- **[稳定的自定义操作 SPI](https://zhongkechen.github.io/async-durable-execution/custom-operations.html)** - 第三方软件包可以预留不透明且确定性的原语标识、使用自定义子类型，并在无需导入 SDK 内部模块的情况下构建有状态且重放安全的操作。
- **[声明式 DAG 工作流](https://zhongkechen.github.io/async-durable-execution/api/extension/flow.html#quick-start)** - 使用带类型的节点输入、推导或条件依赖、失败路由和节点内持久操作来定义无环工作流。SDK 会在执行前验证图，并跳过所选输出不依赖的节点。
- **[感知暂停的清理与补偿](https://zhongkechen.github.io/async-durable-execution/terminal-scopes.html)** - 注册持久清理和逆序补偿，使其只在逻辑完成或失败时运行，而不会在等待、回调、重试或分支仅仅暂停时触发。
- **[后台操作任务](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#background-operation-tasks)** - `step(...)`、`wait(...)`、`invoke(...)`、`recurse(...)`、`run_in_child_context(...)`、`terminal_scope(...)` 与 `flow(...)` 等持久操作会返回 `asyncio.Task` 对象，因此独立操作可以在后台运行，并通过 `asyncio.gather` 一起等待，无需使用 `parallel()` 或 `map()`。
- **[符合 Python 习惯的操作参数](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html#api-mapping)** - 操作直接使用关键字参数、`datetime.timedelta` 等标准 Python 类型及仅限关键字的名称，无需配置包装对象。
- **[可组合 SerDes 管道](https://zhongkechen.github.io/async-durable-execution/serdes-pipelines.html)** - 串联异步字符串转换，并将大型检查点载荷卸载到 EFS 或 S3 Files，同时提供有界预览和经过验证的不可变存储。
- **[集成本地与云端运行器](https://zhongkechen.github.io/async-durable-execution/api/runner.html)** - 运行器功能现在通过 `async_durable_execution` 提供，包含独立的本地与云端运行器工厂，以及带类型的测试结果辅助对象。
- **[无模型依赖的 Lambda 客户端](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#lambda-client-selection)** - SDK 自主管理 Lambda REST 线格式，不依赖 botocore 服务模型。安装可选的 `httpx` extra 后使用 HTTPX 发送请求；否则相同请求会通过异步适配器使用 botocore 的同步 HTTP 传输。
- **[通过标准库 logging 提供重放感知日志](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html#logging)** - 标准 `logging` logger 会由持久上下文过滤器增强，让工作流程日志在重放时保持安全。
- **[Lambda 层打包](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#lambda-layer-packaging)** - 仓库包含构建与发布 SDK Lambda 层的工具和工作流程，适用于不直接打包依赖项的函数。

<a id="quick-start"></a>

## 🚀 快速开始

安装 SDK：

```console
pip install async-durable-execution
```

如需异步 Lambda 服务客户端，请安装可选的 `httpx` extra：

```console
pip install "async-durable-execution[httpx]"
```

`httpx` extra 会安装 HTTPX，SDK 使用它进行异步、无模型依赖的 Lambda
REST 调用。若未安装，SDK 会通过线程化异步适配器，使用 botocore 的同步
HTTP 传输发送相同的签名请求。Botocore 仍提供 AWS 凭证、端点元数据和
SigV4 签名，但不会使用其生成的 Lambda 服务模型。

之前的 `aioboto` extra 会继续作为 `httpx` 的向后兼容别名提供。

创建 Lambda 持久性函数的事件处理程序：

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

    # 模拟审批（实际场景请使用 wait_for_callback）
    await wait(duration=timedelta(seconds=5), name="await_confirmation")

    receipt = await step(create_receipt(order_id), name="create_receipt")

    return {"status": "approved", "order_id": order_id, "receipt": receipt}
```

持久操作会返回 `asyncio.Task` 对象。如果调用操作后没有立即等待它，该操作会被安排在后台运行，之后仍可等待其结果。这让独立操作可以通过常规 `asyncio` 模式并发执行：

```python
import asyncio

pricing_tasks = [
    step(price_line_item(item), name=f"price-{item['sku']}")
    for item in items
]
priced_items = await asyncio.gather(*pricing_tasks)
```

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
    async with create_local_runner(
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
    async with create_cloud_runner(
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
- `terminal_scope/` 用于回调安全的清理与失败补偿
- `invoke/`（包括 `invoke/recurse.py`）、`with_retry/`、`callback/` 与 `logger_example/` 用于集成与运行行为

如需了解运行或部署示例集成测试的开发者工作流，请参阅[贡献指南](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md#example-integration-tests-and-deployment)。

## 📚 文档

- **[文档网站](https://zhongkechen.github.io/async-durable-execution/)** - 可搜索的指南与从 Python docstring 生成的 API 参考
- **[DAG 工作流 API](https://zhongkechen.github.io/async-durable-execution/api/extension/flow.html)** - 使用 `flow()`、带类型的节点输入、条件依赖和失败路由构建声明式工作流
- **[官方 Python SDK 对比](https://zhongkechen.github.io/async-durable-execution/official-python-sdk-comparison.html)** - 与官方 AWS Durable Execution Python SDK 的并排对比
- **[迁移指南](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html)** - 从官方同步 Python SDK 迁移到这个异步优先 SDK
- **[工作流模式](https://zhongkechen.github.io/async-durable-execution/workflow-patterns.html)** - 构建智能体循环、人工审批工作流和补偿事务
- **[持久终止作用域](https://zhongkechen.github.io/async-durable-execution/terminal-scopes.html)** - 在逻辑终止结果时运行清理与补偿，而不把暂停视为失败
- **[部署与调用](https://zhongkechen.github.io/async-durable-execution/deployment.html)** - 配置 IAM、限定函数标识符、调用、CloudFormation 与 SAM
- **[使用同步代码](https://zhongkechen.github.io/async-durable-execution/using-synchronous-code.html)** - 安全包装既有同步业务逻辑与阻塞式客户端
- **[高级用法](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html)** - 了解后台操作任务、批量完成条件、Lambda 客户端与 Lambda 层
- **[自定义持久操作](https://zhongkechen.github.io/async-durable-execution/custom-operations.html)** - 基于稳定的扩展作者接口构建第三方持久操作库
- **[运行器架构](https://zhongkechen.github.io/async-durable-execution/runner-architecture.html)** - 本地与云端运行器的执行流程、组件与图表
- **[贡献指南](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md)** - 开发工作流、Hatch 命令、测试与 pull request 指南

## 参考资料

- **[AWS Durable Execution 文档](https://docs.aws.amazon.com/durable-execution/)** - 概念、入门、核心操作、高级主题与 API 参考
- **[Lambda 持久性函数指南](https://docs.aws.amazon.com/zh_cn/lambda/latest/dg/durable-functions.html)** - Lambda 持久性函数的工作方式

## 💬 反馈与支持

- [Bug 报告](https://github.com/zhongkechen/async-durable-execution/issues/new?template=bug_report.yml)
- [功能请求](https://github.com/zhongkechen/async-durable-execution/issues/new?template=feature_request.yml)
- [文档反馈](https://github.com/zhongkechen/async-durable-execution/issues/new?template=documentation.yml)
- [贡献指南](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md)

## 📄 许可证

请参阅 [LICENSE](https://github.com/zhongkechen/async-durable-execution/blob/main/LICENSE) 文件以了解本项目的许可信息。

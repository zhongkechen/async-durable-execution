# Python 非同步持久性執行

[![English](https://img.shields.io/badge/Language-English-555555)](https://github.com/zhongkechen/async-durable-execution/blob/main/README.md)
[![简体中文](https://img.shields.io/badge/Language-%E7%AE%80%E4%BD%93%E4%B8%AD%E6%96%87-555555)](https://github.com/zhongkechen/async-durable-execution/blob/main/README.zh-CN.md)
[![Quick start](https://img.shields.io/badge/Quick_start-Python-3776AB?logo=python&logoColor=white)](#quick-start)
[![Read the docs](https://img.shields.io/badge/Read_the_docs-API_reference-0A7BBB)](https://zhongkechen.github.io/async-durable-execution/)

[![Build](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml)
[![Conformance](https://github.com/zhongkechen/async-durable-execution/actions/workflows/conformance-tests.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/conformance-tests.yml)
[![Coverage](https://zhongkechen.github.io/async-durable-execution/coverage/badge.svg)](https://zhongkechen.github.io/async-durable-execution/coverage/)
[![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/zhongkechen/async-durable-execution/blob/main/LICENSE)

**使用原生 `async`/`await` 建置完全符合 AWS Durable Execution 規範且可長時間執行的
AWS Lambda 工作流程。** 自動為狀態建立檢查點，無需持續運算即可暫停，並在故障後恢復執行，
無需執行工作流程伺服器。

## 專案狀態

**完全符合
[AWS Durable Execution 一致性測試套件](https://github.com/aws/aws-durable-execution-conformance-tests)。**
所有上游需求都會在 CI 中針對已部署的 Lambda 函數持續驗證。

專案也維護完整的本機與雲端執行器測試覆蓋，發布產生的
[API 文件](https://zhongkechen.github.io/async-durable-execution/)與
[覆蓋率報告](https://zhongkechen.github.io/async-durable-execution/coverage/)，
並提供非同步耐用工作流程的可執行範例。

> 這是採用 Apache-2.0 授權的
> [AWS Durable Execution Python SDK](https://pypi.org/project/aws-durable-execution-sdk-python/)
> 之社群維護非同步分支。

本專案在保留上游聲明的同時繼續以 Apache License 2.0 發布。

建立這個分支是因為官方 Python SDK 不支援 `async`/`await`，導致它難以與 `asyncio` 函式庫整合。此 SDK 增加了非同步耐用可呼叫物件、背景操作任務、直接的 `asyncio` 任務組合，以及針對現代 Python 應用程式設計的 API。

## ✨ 主要功能

- **[非同步優先的耐用程式碼](https://zhongkechen.github.io/async-durable-execution/official-python-sdk-comparison.html#programming-model)** - 與官方 AWS SDK 相比，使用者提供的耐用事件處理常式、步驟、子內容、`flow` 節點、回呼提交器、`map()` 項目函式、`parallel()` 分支與等待條件檢查都使用 `async def` 撰寫。
- **[官方 SDK 未提供的擴充操作](https://zhongkechen.github.io/async-durable-execution/api/extension/replay_safe.html)** - 本 SDK 新增[重播安全輔助操作](https://zhongkechen.github.io/async-durable-execution/api/extension/replay_safe.html)（`random()`、`now()`、`timestamp()` 與 `uuid()`）和[耐用自我呼叫](https://zhongkechen.github.io/async-durable-execution/api/extension/recurse.html)（`recurse()`）。
- **[宣告式 DAG 工作流程](https://zhongkechen.github.io/async-durable-execution/api/extension/flow.html#quick-start)** - 使用具型別的節點輸入、推導或條件相依性、失敗路由和節點內耐用操作來定義無環工作流程。SDK 會在執行前驗證圖，並略過所選輸出未相依的節點。
- **[背景操作任務](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#background-operation-tasks)** - `step(...)`、`wait(...)`、`invoke(...)`、`recurse(...)`、`run_in_child_context(...)` 與 `flow(...)` 等耐用操作會傳回 `asyncio.Task` 物件，因此獨立操作可以在背景執行，並透過 `asyncio.gather` 一起等待，無需使用 `parallel()` 或 `map()`。
- **[符合 Python 慣例的操作參數](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html#api-mapping)** - 操作直接使用關鍵字引數、`datetime.timedelta` 等標準 Python 型別及僅限關鍵字的名稱，無需組態包裝物件。
- **[整合本機與雲端執行器](https://zhongkechen.github.io/async-durable-execution/api/runner/local.html)** - 執行器功能現在透過 `async_durable_execution` 提供，包含獨立的本機與雲端執行器 factory，以及具型別的測試結果輔助物件。
- **[支援非同步 Lambda 用戶端](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#lambda-client-selection)** - 安裝選用的 `aioboto` extra 即可使用非同步 Lambda 用戶端；否則 SDK 會透過非同步配接器使用內建的同步用戶端。
- **[以標準函式庫 logging 提供重播感知記錄](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html#logging)** - 標準 `logging` logger 會由耐用內容篩選器強化，讓工作流程記錄在重播時保持安全。
- **[Lambda 層打包](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html#lambda-layer-packaging)** - 儲存庫包含建置與發布 SDK Lambda 層的工具和工作流程，適用於不直接封裝相依套件的函式。

<a id="quick-start"></a>

## 🚀 快速開始

安裝 SDK：

```console
pip install async-durable-execution
```

如需非同步 Lambda 服務用戶端，請安裝選用的 `aioboto` extra：

```console
pip install "async-durable-execution[aioboto]"
```

`aioboto` extra 會安裝 `aiobotocore`，讓 SDK 能為持久性執行的檢查點與狀態 API 建立非同步 Lambda 用戶端。若未安裝，SDK 會透過執行緒化的非同步配接器使用內建的 `botocore` 相依套件。

建立 Lambda 耐用函數的事件處理常式：

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

    # 模擬核准（實際情境請使用 wait_for_callback）
    await wait(duration=timedelta(seconds=5), name="await_confirmation")

    receipt = await step(create_receipt(order_id), name="create_receipt")

    return {"status": "approved", "order_id": order_id, "receipt": receipt}
```

耐用操作會傳回 `asyncio.Task` 物件。如果呼叫操作後沒有立即等待它，該操作會被排程在背景執行，之後仍可等待其結果。這讓獨立操作可以透過一般 `asyncio` 模式並行執行：

```python
import asyncio

pricing_tasks = [
    step(price_line_item(item), name=f"price-{item['sku']}")
    for item in items
]
priced_items = await asyncio.gather(*pricing_tasks)
```

## 🧪 測試 Lambda 耐用函數

SDK 包含執行器輔助函式，可用於在本機測試 Lambda 耐用函數，或針對已部署的 Lambda 函式進行測試。本機執行器會在程序內執行事件處理常式，使用記憶體內服務用戶端攔截檢查點操作，並傳回可依操作名稱檢查的 `DurableFunctionTestResult`。

假設上方快速開始的事件處理常式儲存在 `order_workflow.py`，本機測試可以執行同一個 Lambda 耐用函數：

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

將同一個事件處理常式部署到 Lambda 後，請使用雲端執行器測試已部署的 Lambda 耐用函數。函式名稱必須以版本或別名限定，例如 `order-workflow:$LATEST` 或 `order-workflow:prod`。

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

## 🧩 範例

Lambda 耐用函數範例位於 `examples/`。可以從 `hello_world.py` 開始，它是最小的完整事件處理常式。

`test_examples/` 中的範例測試也很適合作為可執行的範本參考。可依操作或模式瀏覽：

- `step/`、`wait/`、`wait_for_callback/` 與 `wait_for_condition/` 用於核心耐用操作
- `step/steps_with_gather.py` 展示如何啟動多個步驟任務，並透過 `asyncio.gather` 一起等待
- `flow/`、`map/`、`parallel/` 與 `run_in_child_context/` 用於組合模式
- `invoke/`（包括 `invoke/recurse.py`）、`with_retry/`、`callback/` 與 `logger_example/` 用於整合與執行行為

如需了解執行或部署範例整合測試的開發者工作流程，請參閱[貢獻指南](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md#example-integration-tests-and-deployment)。

## 📚 文件

- **[文件網站](https://zhongkechen.github.io/async-durable-execution/)** - 可搜尋的指南與從 Python docstring 產生的 API 參考
- **[DAG 工作流程 API](https://zhongkechen.github.io/async-durable-execution/api/extension/flow.html)** - 使用 `flow()`、具型別的節點輸入、條件相依性和失敗路由建構宣告式工作流程
- **[官方 Python SDK 比較](https://zhongkechen.github.io/async-durable-execution/official-python-sdk-comparison.html)** - 與官方 AWS Durable Execution Python SDK 的並排比較
- **[遷移指南](https://zhongkechen.github.io/async-durable-execution/migrating-from-official-python-sdk.html)** - 從官方同步 Python SDK 遷移到這個非同步優先 SDK
- **[使用同步程式碼](https://zhongkechen.github.io/async-durable-execution/using-synchronous-code.html)** - 安全包裝既有同步業務邏輯與阻塞式用戶端
- **[進階用法](https://zhongkechen.github.io/async-durable-execution/advanced-usage.html)** - 瞭解背景操作任務、批次完成條件、Lambda 用戶端與 Lambda 層
- **[執行器架構](https://zhongkechen.github.io/async-durable-execution/runner-architecture.html)** - 本機與雲端執行器的執行流程、元件與圖表
- **[貢獻指南](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md)** - 開發工作流程、Hatch 指令、測試與 pull request 指南

## 參考資料

- **[AWS 持久性執行 SDK 開發人員指南](https://docs.aws.amazon.com/durable-execution/)** - 概念、入門、核心操作、進階主題與 API 參考
- **[Lambda 耐用函數指南](https://docs.aws.amazon.com/zh_tw/lambda/latest/dg/durable-functions.html)** - Lambda 耐用函數的運作方式

## 💬 意見回饋與支援

- [Bug 回報](https://github.com/zhongkechen/async-durable-execution/issues/new?template=bug_report.yml)
- [功能請求](https://github.com/zhongkechen/async-durable-execution/issues/new?template=feature_request.yml)
- [文件意見回饋](https://github.com/zhongkechen/async-durable-execution/issues/new?template=documentation.yml)
- [貢獻指南](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md)

## 📄 授權

請參閱 [LICENSE](https://github.com/zhongkechen/async-durable-execution/blob/main/LICENSE) 檔案以了解本專案的授權資訊。

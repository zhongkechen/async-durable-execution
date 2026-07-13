# Python 非同步持久性執行

[English](README.md) | [简体中文](README.zh-CN.md)

[![Build](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml/badge.svg)](https://github.com/zhongkechen/async-durable-execution/actions/workflows/build.yml)
[![API Docs](https://img.shields.io/badge/API%20Docs-GitHub%20Pages-0A7BBB)](https://zhongkechen.github.io/async-durable-execution/)
[![Coverage](https://zhongkechen.github.io/async-durable-execution/coverage/badge.svg)](https://zhongkechen.github.io/async-durable-execution/coverage/)
[![PyPI - Version](https://img.shields.io/pypi/v/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/async-durable-execution.svg)](https://pypi.org/project/async-durable-execution)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/zhongkechen/async-durable-execution/badge)](https://scorecard.dev/viewer/?uri=github.com/zhongkechen/async-durable-execution)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

-----

使用具備檢查點的步驟 (`step`)、等待 (`wait`)、回呼與平行執行，建置可靠、長時間執行的 Lambda 耐用函數。

本儲存庫是原 Apache-2.0 授權 [AWS Durable Execution Python SDK](https://pypi.org/project/aws-durable-execution-sdk-python/) 的社群維護分支，並在保留上游聲明的同時，繼續以 Apache License 2.0 發布。

建立這個分支首先是因為官方 Python SDK 不支援 `async`/`await`，導致它很難與其他 `asyncio` 函式庫良好協作。這個分支特別著重於讓非同步 Python 能自然地搭配 Lambda 耐用函數使用。其次，官方 Python SDK 也缺少原生背景操作任務和直接的 `asyncio` 任務組合。這個 SDK 解決了這些問題，公開 API 在耐用操作邊界仍維持同步介面，使用者提供的耐用可呼叫物件必須對事件處理常式、步驟、子內容、回呼提交器與條件檢查使用 `async def`，並採用更 Pythonic 的風格，讓 API 在現代 Python 程式碼中用起來更自然。

## ✨ 主要功能

- **非同步優先的耐用程式碼** - 與官方 AWS SDK 相比，使用者提供的耐用事件處理常式、步驟、子內容、回呼提交器、`map()` 項目函式、`parallel()` 分支與等待條件檢查都使用 `async def` 撰寫。
- **可 await 的耐用操作** - 工作流程程式碼現在使用可 await 的輔助函式，例如 `step(...)`、`wait(...)`、`invoke(...)`、`map(...)`、`parallel(...)` 與 `run_in_child_context(...)`。
- **簡化的耐用操作 API** - `v2` API 移除組態包裝物件，改用直接的關鍵字引數與更清楚的呼叫位置，包括僅限關鍵字的操作名稱。
- **整合本機與雲端執行器** - 執行器功能現在透過 `async_durable_execution` 提供，包含獨立的本機與雲端執行器 factory，以及具型別的測試結果輔助物件。
- **支援非同步 Lambda 用戶端** - 安裝選用的 `aioboto` extra 即可使用非同步 Lambda 用戶端；否則 SDK 會透過非同步配接器使用內建的同步用戶端。
- **以標準函式庫 logging 提供重播感知記錄** - 標準 `logging` logger 會由耐用內容篩選器強化，讓工作流程記錄在重播時保持安全。
- **Lambda 層打包** - 儲存庫包含建置與發布 SDK Lambda 層的工具和工作流程，適用於不直接封裝相依套件的函式。
- **更完整的驗證與文件** - 專案現在包含擴充後的本機/雲端執行器覆蓋、產生的 API 文件、覆蓋率發布，以及針對非同步 Lambda 耐用函數更新的範例。

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

    # 模擬核准（實際情境請使用 wait_for_callback）
    await wait(duration=timedelta(seconds=5), name="await_confirmation")

    receipt = await step(create_receipt(order_id), name="create_receipt")

    return {"status": "approved", "order_id": order_id, "receipt": receipt}
```

SDK 接受使用者程式碼的所有位置都必須使用非同步可呼叫物件，包括 `map()` 項目函式、繫結的 `parallel()` 分支可呼叫物件、子內容、回呼提交器與等待條件檢查。這些可呼叫物件可以是函式、實例方法、類別方法或靜態方法。耐用內容操作是可 await 的，並與事件處理常式在同一個 event loop 上執行。

事件處理常式輸入會在你的程式碼執行前，先從持久性執行有效載荷反序列化。空白或只包含空白字元的有效載荷會正規化為 `{}`，格式錯誤的 JSON 則會在使用者程式碼執行前讓呼叫失敗。

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

Lambda 耐用函數範例位於 `async-durable-execution-examples/src/async_durable_execution_examples/`。可以從 `hello_world.py` 開始，它是最小的完整事件處理常式。

`async-durable-execution-examples/test_examples/` 中的範例測試也很適合作為可執行的範本參考。可依操作或模式瀏覽：

- `step/`、`wait/`、`wait_for_callback/` 與 `wait_for_condition/` 用於核心耐用操作
- `map/`、`parallel/` 與 `run_in_child_context/` 用於組合模式
- `invoke/`、`with_retry/`、`callback/` 與 `logger_example/` 用於整合與執行行為

如需了解執行或部署範例整合測試的開發者工作流程，請參閱[貢獻指南](CONTRIBUTING.md#example-integration-tests-and-deployment)。

## 📚 文件

- **[產生的 API 參考](https://zhongkechen.github.io/async-durable-execution/)** - 從 Python docstring 自動產生，並透過 GitHub Pages 發布
- **[官方 Python SDK 比較](docs/official-python-sdk-comparison.md)** - 與官方 AWS Durable Execution Python SDK 的並排比較
- **[遷移指南](docs/migrating-from-official-python-sdk.md)** - 從官方同步 Python SDK 遷移到這個非同步優先 SDK
- **[使用同步程式碼](docs/using-synchronous-code.md)** - 安全包裝既有同步業務邏輯與阻塞式用戶端
- **[進階用法](docs/advanced-usage.md)** - 設定批次完成條件、Lambda 用戶端與 Lambda 層
- **[執行器架構](docs/runner-architecture.md)** - 本機與雲端執行器的執行流程、元件與圖表
- **[貢獻指南](CONTRIBUTING.md)** - 開發工作流程、Hatch 指令、測試與 pull request 指南

## 參考資料

- **[AWS 持久性執行 SDK 開發人員指南](https://docs.aws.amazon.com/durable-execution/)** - 概念、入門、核心操作、進階主題與 API 參考
- **[Lambda 耐用函數指南](https://docs.aws.amazon.com/zh_tw/lambda/latest/dg/durable-functions.html)** - Lambda 耐用函數的運作方式

## 💬 意見回饋與支援

- [Bug 回報](https://github.com/zhongkechen/async-durable-execution/issues/new?template=bug_report.yml)
- [功能請求](https://github.com/zhongkechen/async-durable-execution/issues/new?template=feature_request.yml)
- [文件意見回饋](https://github.com/zhongkechen/async-durable-execution/issues/new?template=documentation.yml)
- [貢獻指南](CONTRIBUTING.md)

## 📄 授權

請參閱 [LICENSE](LICENSE) 檔案以了解本專案的授權資訊。

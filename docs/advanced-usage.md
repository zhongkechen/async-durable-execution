# Advanced Usage

## Background Operation Tasks

Durable operation helpers such as `step()`, `wait()`, `invoke()`,
`run_in_child_context()`, `wait_for_callback()`, `wait_for_condition()`, and
`with_retry()` return `asyncio.Task` objects. Awaiting an operation directly still
works:

```python
result = await step(fetch_order(order_id), name="fetch-order")
```

Because the operation is a task, you can also start several independent operations
and let them run in the background before awaiting them together. This uses normal
`asyncio` task patterns and does not require `parallel()` or `map()`:

```python
import asyncio

pricing_tasks = [
    step(fetch_item(item_id), name=f"fetch-item-{item_id}")
    for item_id in item_ids
]
items = await asyncio.gather(*pricing_tasks)
```

The same pattern works with different operation types when they are independent:

```python
delay_task = wait(duration=timedelta(seconds=30), name="cooldown")
invoke_task = invoke(
    function_name=processor_function,
    payload={"order_id": order_id},
    name="process-order",
)

_, invoke_result = await asyncio.gather(delay_task, invoke_task)
```

On Python 3.12 and newer, the SDK uses `asyncio.eager_task_factory` when creating
operation tasks. This starts the operation task immediately and runs it until its
first suspension point, making task creation order deterministic.

On Python 3.10 and 3.11, `asyncio` does not provide eager task start. The SDK falls
back to normal lazy task creation with `loop.create_task()`. Eager start is treated as
a performance and ordering optimization only; the durable operation contract is still
that the helper returns an `asyncio.Task` that can run in the background and be awaited
later.

## Lambda Client Selection

The SDK chooses a Lambda API client for durable checkpoint and state APIs based on the installed dependencies.

With the optional `aioboto` extra, the SDK creates an async Lambda client by default:

```console
pip install "async-durable-execution[aioboto]"
```

Without the extra, the SDK uses the bundled `botocore` dependency through a threaded async adapter.

Explicitly provided Lambda API clients are detected as sync or async and wrapped accordingly. Code that must force the sync `botocore` client can create one explicitly and pass it to the durable handler:

```python
from async_durable_execution import create_default_sync_client, durable_execution

lambda_client = create_default_sync_client()


@durable_execution(boto3_client=lambda_client)
async def handler(event: dict) -> dict:
    return {"ok": True}
```

## Lambda Layer Packaging

Use a Lambda layer when you want multiple durable functions to share the SDK instead of vendoring it in each function zip.

Build a local layer archive from this checkout:

```console
hatch run python scripts/build_layer.py \
  --sdk-source async-durable-execution \
  --output dist/async-durable-execution-layer.zip
```

You can also use a prebuilt layer published by GitHub Actions. The layer ARN is shown in the summary of the [Lambda layer publish workflow](https://github.com/zhongkechen/async-durable-execution/actions/workflows/lambda-layer-publish.yml).

Publish the zip as an `AWS::Serverless::LayerVersion` or `AWS::Lambda::LayerVersion`, then add the layer ARN to Python durable functions.

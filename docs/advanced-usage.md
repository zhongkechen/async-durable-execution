# Advanced Usage

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

# Advanced Usage: Lambda Layer Packaging

Use a Lambda layer when you want multiple durable functions to share the SDK instead of vendoring it in each function zip.

Build a local layer archive from this checkout:

```console
hatch run python scripts/build_layer.py \
  --sdk-source async-durable-execution \
  --output dist/async-durable-execution-layer.zip
```

You can also publish the repository-built layer from GitHub Actions.

Publish the zip as an `AWS::Serverless::LayerVersion` or `AWS::Lambda::LayerVersion`, then add the layer ARN to Python durable functions.

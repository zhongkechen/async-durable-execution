# Async Durable Execution Lambda Layer

This package builds an AWS Lambda layer zip that vendors
`async-durable-execution` under the Lambda layer `python/` directory.
Attach the published layer to a Python Lambda function and the function code can
import `async_durable_execution` without bundling the SDK in the function zip.

## Build a Layer Zip

```console
pip install async-durable-execution-lambda-layer
async-durable-execution-build-layer --output dist/async-durable-execution-layer.zip
```

By default, the builder installs the matching SDK version:

```text
async-durable-execution==<package version>
```

For local development from this monorepo, point the builder at the local SDK
package:

```console
hatch run python -m async_durable_execution_lambda_layer.builder \
  --sdk-source ../async-durable-execution \
  --output dist/async-durable-execution-layer.zip
```

Additional pip arguments can be passed after `--pip-arg`, for example:

```console
async-durable-execution-build-layer \
  --output dist/async-durable-execution-layer.zip \
  --pip-arg=--only-binary=:all:
```

## Publish with SAM

Create a SAM resource that points at the generated zip:

```yaml
Resources:
  AsyncDurableExecutionLayer:
    Type: AWS::Serverless::LayerVersion
    Properties:
      LayerName: async-durable-execution
      Description: async-durable-execution SDK for Python durable Lambda functions
      ContentUri: dist/async-durable-execution-layer.zip
      CompatibleRuntimes:
        - python3.10
        - python3.11
        - python3.12
        - python3.13
        - python3.14
      RetentionPolicy: Retain

  WorkflowFunction:
    Type: AWS::Serverless::Function
    Properties:
      Runtime: python3.14
      Handler: app.handler
      CodeUri: src/
      DurableConfig:
        ExecutionTimeout: 3600
        RetentionPeriodInDays: 7
      Layers:
        - !Ref AsyncDurableExecutionLayer
```

Durable functions still need durable execution enabled on the Lambda function and
the required IAM permissions, including checkpoint permissions.

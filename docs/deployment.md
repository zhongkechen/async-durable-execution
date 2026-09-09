# Deploy and Invoke

Deploying a durable function requires more than uploading the handler:

1. Enable durable execution with `DurableConfig`.
2. Grant the function's execution role permission to checkpoint and restore
   durable state.
3. Publish a version or create an alias so callers can use a qualified function
   identifier.

Use numbered versions or aliases for production. Reserve `$LATEST` for
development and testing.

## Python 3.15 Lambda Preview

The Python 3.15 Lambda preview currently lacks the built-in `sentinel` that
AnyIO 4.15 assumes is available on Python 3.15. With that dependency combination,
HTTPX initialization or cleanup can raise `NameError: name 'sentinel' is not defined`.

For Python 3.15, the SDK's `httpx` extra and legacy `aioboto` alias therefore
require AnyIO `>=4.14.2,<4.15`. Rebuild existing Lambda layers using the updated
SDK requirements. This constraint does not apply to other Python versions and
can be revisited when the Lambda runtime includes the required builtin.

Pip evaluates Python-version markers using the build interpreter. For the shared
release layer advertised for Python 3.10–3.15, the release workflow therefore
pins AnyIO 4.14.2 explicitly while building on Python 3.10. It tests the resulting
ZIP on every advertised runtime before publication. Building a layer on another
Python version does not automatically apply the Python 3.15 package constraint.

## IAM Permissions

Attach the AWS managed
[`AWSLambdaBasicDurableExecutionRolePolicy`](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AWSLambdaBasicDurableExecutionRolePolicy.html)
to the function's execution role. It includes the CloudWatch Logs permissions
used by Lambda and these durable execution permissions:

- `lambda:CheckpointDurableExecutions`
- `lambda:GetDurableExecutionState`

Add `lambda:InvokeFunction` for each target called through `invoke()`. Recursive
self-invocation with `recurse()` requires the same permission on the current
function.

An external principal that completes callbacks needs the applicable Lambda
callback permissions:

- `lambda:SendDurableExecutionCallbackSuccess`
- `lambda:SendDurableExecutionCallbackFailure`
- `lambda:SendDurableExecutionCallbackHeartbeat` when it sends heartbeats

Scope invoke and callback permissions to the functions and executions that the
principal is allowed to access.

## Qualified Function Identifiers

Durable functions must be invoked with a version, alias, or `$LATEST`.

Valid identifiers include:

```text
arn:aws:lambda:us-east-1:123456789012:function:order-workflow:1
arn:aws:lambda:us-east-1:123456789012:function:order-workflow:prod
arn:aws:lambda:us-east-1:123456789012:function:order-workflow:$LATEST
order-workflow:1
order-workflow:prod
```

An unqualified function name or ARN is not valid for a durable invocation:

```text
arn:aws:lambda:us-east-1:123456789012:function:order-workflow
order-workflow
```

The same qualification rule applies to `invoke()`, `recurse()`, and the cloud
test runner.

## Execution Version Pinning

When Lambda creates a durable execution, it resolves the qualified function
identifier to a specific published function version. The execution and its
persisted durable state remain associated with that immutable version for the
execution's lifetime. Every replay or resume therefore uses the same handler
artifact and bundled SDK version that created its checkpoints.

Deploying updated code or an updated SDK publishes a new function version.
Moving an alias to that version affects only durable executions created after
the alias update. Existing executions continue on their original version, so
the new SDK version is not used to deserialize their checkpoint data. Account
for this version isolation when evaluating serialization compatibility across
SDK releases. Compatibility is still required within one deployed artifact and
for data explicitly exchanged across versioned functions.

`$LATEST` is the exception because it is mutable. Updating `$LATEST` can make an
in-flight execution replay with different handler code or a different SDK
version. Do not use `$LATEST` for production durable executions.

## Invoke from the AWS CLI

A synchronous invocation waits for the result and is limited to 15 minutes:

```console
aws lambda invoke \
  --function-name order-workflow:prod \
  --cli-binary-format raw-in-base64-out \
  --payload '{"orderId":"12345"}' \
  response.json
```

Use an asynchronous invocation for long-running work:

```console
aws lambda invoke \
  --function-name order-workflow:prod \
  --invocation-type Event \
  --cli-binary-format raw-in-base64-out \
  --payload '{"orderId":"12345"}' \
  response.json
```

Supply a durable execution name when the caller needs idempotent execution
creation. Reusing the name does not create another execution:

```console
aws lambda invoke \
  --function-name order-workflow:prod \
  --invocation-type Event \
  --durable-execution-name order-processing-12345 \
  --cli-binary-format raw-in-base64-out \
  --payload '{"orderId":"12345"}' \
  response.json
```

## AWS CloudFormation

The following resources configure a function, publish a version, and expose it
through the `prod` alias. Upload an application bundle containing the handler
and its dependencies, then pass its bucket and key as stack parameters.

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Parameters:
  CodeBucket:
    Type: String
  CodeKey:
    Type: String

Resources:
  DurableFunctionRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy

  DurableFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: order-workflow
      Runtime: python3.14
      Handler: index.handler
      Role: !GetAtt DurableFunctionRole.Arn
      Code:
        S3Bucket: !Ref CodeBucket
        S3Key: !Ref CodeKey
      DurableConfig:
        ExecutionTimeout: 3600
        RetentionPeriodInDays: 7

  DurableFunctionVersion:
    Type: AWS::Lambda::Version
    Properties:
      FunctionName: !Ref DurableFunction

  DurableFunctionAlias:
    Type: AWS::Lambda::Alias
    Properties:
      FunctionName: !Ref DurableFunction
      FunctionVersion: !GetAtt DurableFunctionVersion.Version
      Name: prod
```

## AWS SAM

`AutoPublishAlias` publishes a version and creates the qualified alias during
deployment:

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31

Resources:
  DurableFunction:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: order-workflow
      Runtime: python3.14
      Handler: index.handler
      CodeUri: ./src
      DurableConfig:
        ExecutionTimeout: 3600
        RetentionPeriodInDays: 7
      Policies:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy
      AutoPublishAlias: prod
```

The repository's example deployment and cloud-test commands are documented in
the [contributing guide](https://github.com/zhongkechen/async-durable-execution/blob/main/CONTRIBUTING.md#example-integration-tests-and-deployment).

## AWS References

- [AWS Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
- [Invoking durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-invoking.html)
- [Deploying with infrastructure as code](https://docs.aws.amazon.com/lambda/latest/dg/durable-getting-started-iac.html)
- [Lambda API reference](https://docs.aws.amazon.com/lambda/latest/api/)

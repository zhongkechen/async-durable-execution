from async_durable_execution.operation import childfrom async_durable_execution.operation import stepfrom async_durable_execution.operation import stepfrom async_durable_execution.operation import stepfrom async_durable_execution.operation import step

# AWS Lambda Durable Functions SDK - Agent Guide

> Build resilient, long-running AWS Lambda functions with automatic state persistence, retry logic, and workflow orchestration.

## Overview

AWS Lambda durable functions extend Lambda's programming model to build multi-step applications and AI workflows with automatic state persistence. Applications can run for days or months, survive failures, and only incur charges for actual compute time.

**Packages:**

- **Python SDK**: `async-durable-execution`
- **Python test runner**: `async-durable-execution-runner`
- **Python examples**: `async-durable-execution-examples`

**Core Primitives:**

- **Steps** - Execute business logic with automatic checkpointing and transparent retries
- **Waits** - Suspend execution without compute charges (for delays, human approvals, scheduled tasks)
- **Durable Invokes** - Reliable function chaining for modular, composable architectures

## Critical Rules

### ⚠️ The Replay Model

Durable functions use a "replay" execution model. On replay (after wait/failure/resume), code runs from the beginning. Steps that already completed return their checkpointed results WITHOUT re-executing. Code OUTSIDE steps executes again on every replay.

### Rule 1: Deterministic Code Outside Steps

ALL code outside steps MUST be deterministic.

```python
# ❌ WRONG: Non-deterministic code outside steps
id = str(uuid.uuid4())  # Different on each replay!
timestamp = time.time()  # Different on each replay!

# ✅ CORRECT: Non-deterministic code inside steps
id = step.step(lambda: str(uuid.uuid4()), name="generate-id")
timestamp = context.step(lambda: time.time(), name="get-time")
```

**Must be in steps:** `time.time()`, `random.random()`, UUID generation, API calls, database queries, file system operations.

### Rule 2: No Nested Durable Operations

You CANNOT call durable operations inside a step function.

```python
# ❌ WRONG: Nested durable operations
async def process():
    context.wait(duration=timedelta(seconds=1))  # ERROR!


# ✅ CORRECT: Use run_in_child_context for grouping
async def process(child_ctx: DurableContext):
    child_ctx.wait(duration=timedelta(seconds=1))
    child_ctx.step(some_step)


child.run_in_child_context(process, name="process")
```

### Rule 3: Closure Mutations Are Lost on Replay

Variables mutated inside steps are NOT preserved across replays.

```python
# ❌ WRONG: Counter mutations lost
counter = 0
async def increment():
    nonlocal counter
    counter += 1
context.step(increment)
print(counter)  # 0 on replay!

# ✅ CORRECT: Return values from steps
counter = context.step(lambda: counter + 1, name="increment")
```

### Rule 4: Side Effects Outside Steps Repeat

Side effects (logging, API calls) outside steps happen on EVERY replay.

**Exception:** `context.logger` is replay-aware and safe to use anywhere.

```python
# ❌ WRONG
print("Starting")  # Prints multiple times!
send_email(...)    # Sends multiple emails!

# ✅ CORRECT
context.logger.info("Starting")  # Deduplicated automatically
context.step(lambda: send_email(...), name="email")
```

## IAM Permissions

Durable functions require the [`AWSLambdaBasicDurableExecutionRolePolicy`](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AWSLambdaBasicDurableExecutionRolePolicy.html) managed policy, which includes:

- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` - CloudWatch Logs
- `lambda:CheckpointDurableExecutions` - Persist execution state
- `lambda:GetDurableExecutionState` - Retrieve execution state

For durable invokes (calling other durable functions), also add:

- `lambda:InvokeFunction` on the target function ARN

For callbacks from external systems:

- External systems need `lambda:SendDurableExecutionCallbackSuccess` and `lambda:SendDurableExecutionCallbackFailure`

## Invoking Durable Functions

### Qualified ARNs Required

Durable functions **require qualified identifiers** for invocation. You must use a version number, alias, or `$LATEST`.

**✅ Valid invocations:**

```bash
# Full ARN with version
arn:aws:lambda:us-east-1:123456789012:function:my-function:1

# Full ARN with alias
arn:aws:lambda:us-east-1:123456789012:function:my-function:prod

# Full ARN with $LATEST
arn:aws:lambda:us-east-1:123456789012:function:my-function:$LATEST

# Function name with version/alias
my-function:1
my-function:prod
```

**❌ Invalid invocations:**

```bash
# Unqualified ARN - NOT ALLOWED
arn:aws:lambda:us-east-1:123456789012:function:my-function

# Unqualified function name - NOT ALLOWED
my-function
```

### Invocation Methods

**Synchronous** - Wait for response (limited to 15 minutes):

```bash
aws lambda invoke \
  --function-name my-durable-function:1 \
  --cli-binary-format raw-in-base64-out \
  --payload '{"orderId": "12345"}' \
  response.json
```

**Asynchronous** - Fire-and-forget (supports up to 1 year execution):

```bash
aws lambda invoke \
  --function-name my-durable-function:1 \
  --invocation-type Event \
  --cli-binary-format raw-in-base64-out \
  --payload '{"orderId": "12345"}' \
  response.json
```

**Idempotent invocation** - Use `--durable-execution-name` to ensure the same execution is never created twice:

```bash
aws lambda invoke \
  --function-name my-durable-function:1 \
  --invocation-type Event \
  --durable-execution-name "order-processing-12345" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"orderId": "12345"}' \
  response.json
```

**Best Practice:** Use numbered versions or aliases for production. Use `$LATEST` only for development/prototyping.

## SDK API Reference

### Handler Wrapper

```python
from async_durable_execution import durable_execution


@durable_execution
async def handler(event: dict) -> dict:
    # Your durable workflow
    return result
```

### Steps - Atomic Operations

```python
from functools import partial
from datetime import timedelta

from async_durable_execution import step
from async_durable_execution import RetryStrategyBuilder


async def fetch_user(user_id: str) -> dict:
    return {"id": user_id, "name": "Jane"}


# Execute step (uses function name automatically)
result = step(partial(fetch_user, user_id))

# Named step with lambda
result = step(lambda: fetch_data(), name="fetch-user")

# With retry configuration
retry_config = RetryStrategyBuilder(
    max_attempts=3,
    initial_delay=timedelta(seconds=1),
    backoff_rate=2.0,
)
result = step(
    partial(fetch_user, user_id),
    retry_strategy=retry_config.build(),
)
```

### Wait - Pause Execution

```python
from datetime import timedelta
from async_durable_execution import wait

wait(duration=timedelta(seconds=30))
wait(duration=timedelta(hours=1))
wait(duration=timedelta(days=7), name="rate-limit-delay")
```

### Invoke - Call Other Functions

Invoke another durable Lambda function. **Must use qualified function name** (with version or alias).

```python
import os

result = invoke(
    function_name=os.environ["PAYMENT_PROCESSOR_ARN"],
    payload={"amount": 100, "currency": "USD"},
    name="process-payment"
)
```

### Child Context - Group Operations

```python
async def process_order() -> dict:
    validated = step(validate_step(data), name="validate")
    wait(duration=timedelta(seconds=1))
    processed = step(process_step(validated), name="process")
    return processed

result = run_in_child_context(process_order, name="process-order")
```

### Wait for Callback - External Integration

```python
from async_durable_execution import get_current_context


async def submit_approval():
    callback_id = get_current_context().callback_id
    send_approval_email(callback_id)


result = wait_for_callback(
    submitter=submit_approval,
    timeout=timedelta(hours=24),
    name="wait-for-approval"
)
```

### Wait for Condition - Polling

```python
from async_durable_execution import WaitStrategyBuilder
from async_durable_execution import WaitForConditionDecision


async def check_job(state: dict, check_ctx) -> dict:
    status = get_job_status(state["job_id"])
    return {"job_id": state["job_id"], "status": status}


result = wait_for_condition(
    check=check_job,
    initial_state={"job_id": "job-123", "status": "pending"},
    wait_strategy=WaitStrategyBuilder(
        should_continue_polling=lambda state: state["status"] != "completed",
        initial_delay=timedelta(seconds=2),
    ).build(),
    name="wait-for-job"
)
```

### Map - Process Arrays

```python
from async_durable_execution import CompletionConfig, get_current_context


async def process_item(item: dict) -> dict:
    map_context = get_current_context()
    return step(lambda: process(item), name=f"process-{map_context.index}")


results = map(
    func=process_item,
    items=items,
    max_concurrency=5,
    completion_config=CompletionConfig(
        min_successful=8,
        tolerated_failure_count=2
    ),
    name="process-items"
)

results.throw_if_error()
all_results = results.get_results()
```

### Parallel - Parallel Branches

```python
from async_durable_execution import durable_callable


@durable_callable
async def task1(user_id: str):
    @durable_callable
    async def fetch():
        return fetch_data1(user_id)

    return await step(fetch(), name="fetch1")


@durable_callable
async def task2(user_id: str):
    @durable_callable
    async def fetch():
        return fetch_data2(user_id)

    return await step(fetch(), name="fetch2")


results = await parallel(
    branches=[
        task1(user_id),
        task2(user_id),
    ],
    max_concurrency=2,
    name="parallel-ops"
)
```

## Testing Reference

### Python Setup

```bash
pip install async-durable-execution-runner
```

```python
import pytest
from async_durable_execution_runner import InvocationStatus
from my_module import handler


def test_workflow(durable_runner):
    """Test durable function workflow."""
    with durable_runner(
        handler=handler,
        input={"user_id": "123"},
        timeout=10,
    ) as runner:
        result = runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == {"success": True}

    # Get step by name
    step_result = result.get_step("fetch-user")
    assert step_result.status is InvocationStatus.SUCCEEDED
```

### Testing Key Points

- ✅ Use `result.get_step("name")` - not by index
- ✅ Name all operations for test reliability
- ✅ Ensure callback parameters and step inputs are JSON-serializable
- ✅ Wrap event data in `input={}`

## Common Patterns

### Multi-Step Workflow

```python
@durable_execution
async def handler(event: dict) -> dict:
    validated = context.step(validate_input(event), name="validate")
    processed = context.step(process_data(validated), name="process")
    context.wait(duration=timedelta(seconds=30), name="cooldown")
    context.step(send_notification(processed), name="notify")
    return {"success": True, "data": processed}
```

### GenAI Agent (Agentic Loop)

```python
@durable_execution
async def handler(event: dict) -> str:
    messages = [{"role": "user", "content": event["prompt"]}]

    while True:
        result = step(
            lambda: invoke_ai_model(messages),
            name="invoke-model"
        )

        if result.get("tool") is None:
            return result["response"]

        tool = result["tool"]
        tool_result = step(
            lambda: execute_tool(tool, result["response"]),
            name=f"tool-{tool['name']}"
        )
        messages.append({"role": "assistant", "content": tool_result})
```

### Human-in-the-Loop Approval

```python
@durable_execution
async def handler(event: dict) -> dict:
    plan = step(generate_plan(event), name="generate-plan")

    async def submit_approval():
        callback_id = get_current_context().callback_id
        send_approval_email(event["approver_email"], plan, callback_id)

    answer = wait_for_callback(
        submitter=submit_approval,
        timeout=timedelta(hours=24),
        name="wait-for-approval"
    )

    if answer == "APPROVED":
        step(perform_action(plan), name="execute")
        return {"status": "completed"}
    return {"status": "rejected"}
```

### Saga Pattern (Compensating Transactions)

```python
@durable_execution
async def handler(event: dict) -> dict:
    compensations = []

    try:
        step(book_flight(event), name="book-flight")
        compensations.append(("cancel-flight", lambda: cancel_flight(event)))

        step(book_hotel(event), name="book-hotel")
        compensations.append(("cancel-hotel", lambda: cancel_hotel(event)))

        return {"success": True}
    except Exception as error:
        for name, comp_fn in reversed(compensations):
            step(lambda: comp_fn(), name=name)
        raise error
```

## Infrastructure as Code

Deploy durable functions using CloudFormation or SAM. All require:

1. Enable durable execution on the function
2. Grant checkpoint permissions to the execution role
3. Publish a version or create an alias (qualified ARNs required)

### AWS CloudFormation

```yaml
AWSTemplateFormatVersion: "2010-09-09"
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
      FunctionName: myDurableFunction
      Runtime: python3.14
      Handler: index.handler
      Role: !GetAtt DurableFunctionRole.Arn
      Code:
        ZipFile: |
          # Your durable function code
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

### AWS SAM

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31

Resources:
  DurableFunction:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: myDurableFunction
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

## Related Documentation

**AWS Documentation:**

- [AWS Lambda Durable Functions Guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
- [Lambda API Reference](https://docs.aws.amazon.com/lambda/latest/api/)
- [Invoking Durable Functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-invoking.html)
- [Deploy with IaC](https://docs.aws.amazon.com/lambda/latest/dg/durable-getting-started-iac.html)
- [AWSLambdaBasicDurableExecutionRolePolicy](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AWSLambdaBasicDurableExecutionRolePolicy.html)

**Python SDK:**

- [SDK Repository](https://github.com/zhongkechen/async-durable-execution)
- [SDK README](https://github.com/zhongkechen/async-durable-execution/blob/main/async-durable-execution/README.md)
- [Runner README](https://github.com/zhongkechen/async-durable-execution/blob/main/async-durable-execution-runner/README.md)
- [Examples Package](https://github.com/zhongkechen/async-durable-execution/tree/main/async-durable-execution-examples)

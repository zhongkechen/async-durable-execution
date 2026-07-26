
# AWS Lambda Durable Functions SDK - Agent Guide

> Build resilient, long-running AWS Lambda functions with automatic state persistence, retry logic, and workflow orchestration.

## Overview

AWS Lambda durable functions extend Lambda's programming model to build multi-step applications and AI workflows with automatic state persistence. Applications can run for days or months, survive failures, and only incur charges for actual compute time.

**Packages:**

- **Python SDK and test runner**: `async-durable-execution`
- **Python examples**: `examples`

**Core Operations:**

- **Steps** - Execute business logic with automatic checkpointing and transparent retries
- **Waits** - Suspend execution without compute charges (for delays, human approvals, scheduled tasks)
- **Durable Invokes** - Reliable function chaining for modular, composable architectures
- **Flows** - Define validated acyclic graphs with typed node inputs and conditional dependencies

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
@durable_callable
async def generate_id() -> str:
    return str(uuid.uuid4())


@durable_callable
async def get_time() -> float:
    return time.time()


id = await step(generate_id(), name="generate-id")
timestamp = await step(get_time(), name="get-time")
```

**Must be in steps:** `time.time()`, `random.random()`, UUID generation, API calls, database queries, file system operations.

### Rule 2: No Nested Durable Operations

You CANNOT call durable operations inside a step function.

```python
# ❌ WRONG: Nested durable operations
@durable_callable
async def process_with_nested_operation():
    await wait(duration=timedelta(seconds=1))  # ERROR inside a step!


await step(process_with_nested_operation(), name="process")


# ✅ CORRECT: Use run_in_child_context for grouping
@durable_callable
async def process():
    await wait(duration=timedelta(seconds=1))
    await step(some_step(), name="some-step")


await run_in_child_context(process(), name="process")
```

### Rule 3: Closure Mutations Are Lost on Replay

Variables mutated inside steps are NOT preserved across replays.

```python
# ❌ WRONG: Counter mutations lost
counter = 0


@durable_callable
async def increment():
    nonlocal counter
    counter += 1


await step(increment(), name="increment")
print(counter)  # 0 on replay!

# ✅ CORRECT: Return values from steps
@durable_callable
async def increment(value: int) -> int:
    return value + 1


counter = await step(increment(counter), name="increment")
```

### Rule 4: Side Effects Outside Steps Repeat

Side effects (API calls, writes, and non-replay-aware logging) outside steps happen on EVERY replay.

**Exception:** SDK replay-aware standard logging is safe to use anywhere.

```python
# ❌ WRONG
print("Starting")  # Prints multiple times!
send_email(...)    # Sends multiple emails!

# ✅ CORRECT
logger.info("Starting")  # Deduplicated automatically by the SDK logger filter


@durable_callable
async def send_email_step() -> None:
    send_email(...)


await step(send_email_step(), name="email")
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
from datetime import timedelta

from async_durable_execution import durable_callable
from async_durable_execution import RetryStrategy
from async_durable_execution import step


@durable_callable
async def fetch_user(user_id: str) -> dict:
    return {"id": user_id, "name": "Jane"}


# Execute step (uses function name automatically)
result = await step(fetch_user(user_id))

# Named step
result = await step(fetch_user(user_id), name="fetch-user")

# With retry configuration
retry_strategy = RetryStrategy(
    max_attempts=3,
    initial_delay=timedelta(seconds=1),
    backoff_rate=2.0,
)
result = await step(
    fetch_user(user_id),
    retry_strategy=retry_strategy,
)
```

### Wait - Pause Execution

```python
from datetime import timedelta
from async_durable_execution import wait

await wait(duration=timedelta(seconds=30))
await wait(duration=timedelta(hours=1))
await wait(duration=timedelta(days=7), name="rate-limit-delay")
```

### Invoke - Call Other Functions

Invoke another durable Lambda function. **Must use qualified function name** (with version or alias).

```python
import os

from async_durable_execution import invoke


result = await invoke(
    function_name=os.environ["PAYMENT_PROCESSOR_ARN"],
    payload={"amount": 100, "currency": "USD"},
    name="process-payment"
)
```

### Child Context - Group Operations

```python
from async_durable_execution import (
    durable_callable,
    run_in_child_context,
    step,
    wait,
)


@durable_callable
async def validate_step(data: dict) -> dict:
    return data


@durable_callable
async def process_step(data: dict) -> dict:
    return data


@durable_callable
async def process_order(data: dict) -> dict:
    validated = await step(validate_step(data), name="validate")
    await wait(duration=timedelta(seconds=1))
    processed = await step(process_step(validated), name="process")
    return processed

result = await run_in_child_context(process_order(data), name="process-order")
```

### Flow - Declarative DAG Workflows

Use a synchronous `@durable_dag` function to declare the graph and async
`@durable_node` functions to execute each node. Passing a node projection as an
argument infers the dependency.

```python
from async_durable_execution import (
    durable_dag,
    durable_node,
    flow,
    node,
)


@durable_node
async def load_order(order_id: str) -> dict:
    return {"id": order_id}


@durable_node
async def process_order(order: dict) -> dict:
    return {"id": order["id"], "status": "processed"}


@durable_dag
def order_flow(order_id: str):
    loaded = node(load_order(order_id), name="load-order")
    processed = node(process_order(loaded.outcome), name="process-order")
    return processed.outcome


result = await flow(order_flow("order-123"), name="order-flow")
output = result.output
```

Flow definition code is replayed and must be deterministic. It cannot start
durable operations. Node bodies run in durable child contexts and may call
`step()`, `wait()`, `invoke()`, or nested `flow()` operations. Use `.outcome`,
`.error`, or `.result` projections for success, failure, or any terminal result;
use `a.failed >> b`, `a.completed >> b`, `a & b`, and `a | b` for explicit
conditions. Flows must remain acyclic.

### Wait for Callback - External Integration

```python
from datetime import timedelta

from async_durable_execution import durable_callable
from async_durable_execution import get_current_context
from async_durable_execution import wait_for_callback


@durable_callable
async def submit_approval():
    callback_id = get_current_context().callback_id
    send_approval_email(callback_id)


result = await wait_for_callback(
    submitter=submit_approval(),
    timeout=timedelta(hours=24),
    name="wait-for-approval"
)
```

### Wait for Condition - Polling

```python
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from async_durable_execution import SerDes
from async_durable_execution import WaitDelayStrategy
from async_durable_execution import wait_for_condition


@dataclass(frozen=True)
class JobStatus:
    job_id: str
    attempts: int
    status: str

    def __bool__(self) -> bool:
        return self.status == "completed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "attempts": self.attempts,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobStatus":
        return cls(
            job_id=str(data["job_id"]),
            attempts=int(data["attempts"]),
            status=str(data["status"]),
        )


class JobStatusSerDes(SerDes[JobStatus]):
    async def serialize(self, value: JobStatus) -> str:
        return json.dumps(value.to_dict())

    async def deserialize(self, payload: str) -> JobStatus:
        return JobStatus.from_dict(json.loads(payload))


async def check_job(state: JobStatus | None) -> JobStatus:
    attempts = 1 if state is None else state.attempts + 1
    status = get_job_status("job-123")
    return JobStatus(job_id="job-123", attempts=attempts, status=status)


result = await wait_for_condition(
    check=check_job,
    initial_state=None,
    wait_strategy=WaitDelayStrategy[JobStatus](
        initial_delay=timedelta(seconds=2),
    ),
    serdes=JobStatusSerDes(),
    name="wait-for-job"
)
```

### Map - Process Arrays

```python
from async_durable_execution import CompletionConfig
from async_durable_execution import durable_callable
from async_durable_execution import get_current_context
from async_durable_execution import map
from async_durable_execution import step


@durable_callable
async def process(item: dict) -> dict:
    return item


async def process_item(item: dict) -> dict:
    map_context = get_current_context()
    return await step(process(item), name=f"process-{map_context.index}")


results = await map(
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
from async_durable_execution import parallel
from async_durable_execution import step


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
pip install async-durable-execution
```

```python
from async_durable_execution import InvocationStatus
from async_durable_execution.runner import create_runner
from my_module import handler


async def test_workflow():
    """Test durable function workflow."""
    with create_runner(
        mode="local",
        handler=handler,
        input={"user_id": "123"},
        timeout=10,
    ) as runner:
        result = await runner.run()

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.get_deserialized_result() == {"success": True}

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
@durable_callable
async def validate_input(event: dict) -> dict:
    return event


@durable_callable
async def process_data(data: dict) -> dict:
    return data


@durable_callable
async def send_notification(data: dict) -> None:
    send_notification_api(data)


@durable_execution
async def handler(event: dict) -> dict:
    validated = await step(validate_input(event), name="validate")
    processed = await step(process_data(validated), name="process")
    await wait(duration=timedelta(seconds=30), name="cooldown")
    await step(send_notification(processed), name="notify")
    return {"success": True, "data": processed}
```

### GenAI Agent (Agentic Loop)

```python
@durable_callable
async def invoke_model(messages: list[dict]) -> dict:
    return invoke_ai_model(messages)


@durable_callable
async def run_tool(tool: dict, response: str) -> dict:
    return execute_tool(tool, response)


@durable_execution
async def handler(event: dict) -> str:
    messages = [{"role": "user", "content": event["prompt"]}]

    while True:
        result = await step(invoke_model(messages), name="invoke-model")

        if result.get("tool") is None:
            return result["response"]

        tool = result["tool"]
        tool_result = await step(
            run_tool(tool, result["response"]),
            name=f"tool-{tool['name']}"
        )
        messages.append({"role": "assistant", "content": tool_result})
```

### Human-in-the-Loop Approval

```python
@durable_callable
async def generate_plan(event: dict) -> dict:
    return create_plan(event)


@durable_callable
async def perform_action(plan: dict) -> None:
    execute_plan(plan)


@durable_execution
async def handler(event: dict) -> dict:
    plan = await step(generate_plan(event), name="generate-plan")

    @durable_callable
    async def submit_approval():
        callback_id = get_current_context().callback_id
        send_approval_email(event["approver_email"], plan, callback_id)

    answer = await wait_for_callback(
        submitter=submit_approval(),
        timeout=timedelta(hours=24),
        name="wait-for-approval"
    )

    if answer == "APPROVED":
        await step(perform_action(plan), name="execute")
        return {"status": "completed"}
    return {"status": "rejected"}
```

### Saga Pattern (Compensating Transactions)

```python
@durable_callable
async def book_flight(event: dict) -> None:
    flight_api.book(event)


@durable_callable
async def book_hotel(event: dict) -> None:
    hotel_api.book(event)


@durable_callable
async def cancel_flight(event: dict) -> None:
    flight_api.cancel(event)


@durable_callable
async def cancel_hotel(event: dict) -> None:
    hotel_api.cancel(event)


@durable_execution
async def handler(event: dict) -> dict:
    compensations = []

    try:
        await step(book_flight(event), name="book-flight")
        compensations.append("cancel-flight")

        await step(book_hotel(event), name="book-hotel")
        compensations.append("cancel-hotel")

        return {"success": True}
    except Exception as error:
        for name in reversed(compensations):
            if name == "cancel-flight":
                await step(cancel_flight(event), name=name)
            elif name == "cancel-hotel":
                await step(cancel_hotel(event), name=name)
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
- [Examples](https://github.com/zhongkechen/async-durable-execution/tree/main/examples)

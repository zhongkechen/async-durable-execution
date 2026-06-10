# Integration Tests for Python Durable Execution SDK

This directory contains integration tests for the Python Durable Execution SDK examples. Tests can run in two modes using pytest fixtures.

## Test Modes

### Local Mode (Default)
Tests run against the in-memory `DurableFunctionTestRunner`:
- ✅ Fast execution (seconds)
- ✅ No AWS credentials needed
- ✅ Perfect for development
- ✅ Validates local runner behavior

```bash
# Run all example tests locally (default, from repo root)
hatch run dev-examples:test

# Run with explicit mode flag
pytest --runner-mode=local -m example packages/async-durable-execution-examples/test/

# Run specific test
pytest --runner-mode=local -k test_hello_world packages/async-durable-execution-examples/test/
```

### Cloud Mode (Integration)
Tests run against actual AWS Lambda functions using `DurableFunctionCloudTestRunner`:
- ✅ Validates cloud deployment
- ✅ Tests real Lambda execution
- ✅ Verifies end-to-end behavior
- ⚠️ Requires deployed functions

```bash
# Build the example bundle first (from repo root)
hatch run examples:build

# Generate a one-example SAM template
hatch run examples:generate-sam-template -- --example-name "Hello World"

# Deploy the function with SAM
sam build --template-file packages/async-durable-execution-examples/template.generated.json
sam deploy \
  --template-file .aws-sam/build/template.yaml \
  --stack-name hello-world-test \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --no-confirm-changeset \
  --parameter-overrides \
    FunctionName=HelloWorld-Test \
    LambdaEndpoint=https://lambda.us-west-2.amazonaws.com

# Set environment variables for cloud testing
export AWS_REGION=us-west-2
export LAMBDA_ENDPOINT=https://lambda.us-west-2.amazonaws.com
export PYTEST_FUNCTION_NAME_MAP='{"async_durable_execution_examples.hello_world.handler":"HelloWorld-Test:$LATEST"}'

# Run tests (from repo root)
pytest --runner-mode=cloud -k test_hello_world packages/async-durable-execution-examples/test/

# Or using hatch (from repo root)
hatch run test:examples-integration -k test_hello_world
```

## Writing Tests

Use the `durable_runner` pytest fixture with the `@pytest.mark.durable_execution` marker:

```python
import pytest
from async_durable_execution.execution import InvocationStatus
from examples.src import my_example


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=my_example.handler,
)
def test_my_example(durable_runner):
    """Test my example in both local and cloud modes."""
    with durable_runner:
        result = durable_runner.run(input={"test": "data"}, timeout=10)

    # Assertions work in both modes
    assert result.status == InvocationStatus.SUCCEEDED
    assert result.result == "expected output"

    # Optional mode-specific validations
    if durable_runner.mode == "cloud":
        # Cloud-specific assertions
        pass
```

## Configuration

### Environment Variables (Cloud Mode)
- `AWS_REGION` - AWS region for Lambda invocation (default: us-west-2)
- `LAMBDA_ENDPOINT` - Optional Lambda endpoint URL for testing
- `PYTEST_FUNCTION_NAME_MAP` - JSON mapping of handler identifiers to deployed qualified function names
- `QUALIFIED_FUNCTION_NAME` - Optional fallback for single-function cloud runs

### CLI Options
- `--runner-mode` - Test mode: `local` (default) or `cloud`

### Pytest Markers
- `-m example` - Run only example tests
- `-k test_name` - Run tests matching pattern

## CI/CD Integration

Tests automatically run in CI/CD after deployment:

1. `e2e-tests.yml` generates the SAM template and deploys functions
2. Integration tests run against deployed functions
3. Results reported in GitHub Actions

See `.github/workflows/e2e-tests.yml` for details.

## Troubleshooting

### Timeout errors
**Problem**: `TimeoutError: Execution did not complete within 60s`

**Solution**: Increase timeout in test:
```python
result = runner.run(input="test", timeout=120)  # Increase to 120s
```

### Import errors
**Problem**: `ModuleNotFoundError: No module named 'async_durable_execution_runner'`

**Solution**: Install dependencies:
```bash
hatch run dev-examples:test  # Installs dependencies automatically

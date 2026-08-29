# SerDes Pipelines

A SerDes pipeline contains one value codec followed by zero or more reversible
string stages.

- Serialization runs from the value codec through the stages in declaration
  order.
- Deserialization runs the stages in reverse order, then calls the value
  codec.
- Pipelines are immutable. `then()` returns a new pipeline.

```python
import json

from async_durable_execution import JsonSerDes, SerDesContext


class EnvelopeStage:
    async def serialize(self, value: str, context: SerDesContext) -> str:
        return json.dumps(
            {
                "format": "acme-envelope",
                "version": 1,
                "value": value,
            }
        )

    async def deserialize(self, data: str, context: SerDesContext) -> str:
        parsed = json.loads(data)
        if not isinstance(parsed, dict) or parsed.get("format") != "acme-envelope":
            return data
        if parsed.get("version") != 1 or not isinstance(parsed.get("value"), str):
            raise ValueError("Malformed or unsupported Acme envelope")
        return parsed["value"]


serdes = JsonSerDes().then(EnvelopeStage())
```

Use the pipeline anywhere an operation accepts `serdes`, `serdes_payload`, or
`serdes_result`.

## Self-identifying stages

Every stage must use a self-identifying format. During deserialization, a
stage must:

1. reverse input in its recognized valid format;
2. reject recognized malformed or unsupported input;
3. return unrecognized input unchanged.

Pass-through behavior allows external invocation and callback payloads to reach
the root value codec without pipeline-specific bypass logic.

## Stage context

Stages receive `SerDesContext` explicitly. It includes the durable execution
ARN, entity and operation identity, operation metadata when available, and the
retry attempt when supplied by the caller.

During serialization, `context.original_value` is the Python object passed to
the root value codec. During deserialization it is `None`. Treat the original
value as read-only.

Existing value codecs remain compatible with `get_serdes_context()`. New
pipeline stages should use the explicit context argument.

## Filesystem stage

`FileSystemSerDesStage` stores the preceding stage's string on a durable shared
filesystem.

!!! warning

    Do not use Lambda's ephemeral `/tmp` directory. Replay may run in another
    execution environment where that file does not exist. Use a shared durable
    mount such as Amazon EFS or S3 Files.

```python
from async_durable_execution import (
    FileSystemSerDesMode,
    FileSystemSerDesStage,
    FileSystemSerDesStageConfig,
    JsonSerDes,
    step,
)

result_serdes = JsonSerDes().then(
    FileSystemSerDesStage(
        "/mnt/efs/durable-payloads",
        FileSystemSerDesStageConfig(
            storage_mode=FileSystemSerDesMode.OVERFLOW,
        ),
    )
)

result = await step(
    load_large_order(),
    name="load-large-order",
    serdes=result_serdes,
)
```

`ALWAYS` writes every payload to a file. `OVERFLOW` keeps the complete
versioned envelope inline while it fits the configured checkpoint byte limit
and offloads larger values.

Payload files are immutable and uniquely named. The envelope records the
producer execution and entity, content digest, payload type, and either inline
data or a file path. It also records the exact UTF-8 payload size so
deserialization can reject oversized replacements before reading them into
memory. Deserialization validates the envelope, ownership, path, file type,
symbolic-link boundaries, declared size, and SHA-256 digest before returning
the stored string. Input without the reserved filesystem marker passes through
unchanged.

`FileSystemPathEncoding.URI` creates human-readable execution paths.
`FileSystemPathEncoding.HASH` uses fixed-length SHA-256 path segments.

## Structured previews

A file envelope can include a small structured preview for observability.
Preview rules can include, exclude, or mask fields and enforce a UTF-8 byte
budget.

```python
from async_durable_execution import (
    FileSystemSerDesStage,
    FileSystemSerDesStageConfig,
    JsonSerDes,
    PreviewConfig,
    PreviewField,
    PreviewMode,
)

serdes = JsonSerDes().then(
    FileSystemSerDesStage(
        "/mnt/efs/durable-payloads",
        FileSystemSerDesStageConfig(
            preview_config=PreviewConfig(
                mode=PreviewMode.INCLUDE_ALL,
                exclude=(PreviewField("items"),),
                mask=(PreviewField("email"),),
            )
        ),
    )
)
```

For non-JSON strings or custom preview logic, configure `generate_preview`.
The callback may be synchronous or asynchronous and receives both the
preceding stage string and `SerDesContext`.

```python
async def preview(value: str, context: SerDesContext) -> dict:
    return {
        "operation": context.operation_name,
        "attempt": context.attempt,
    }
```

Preview callbacks are responsible for avoiding disclosure of sensitive values
from both the serialized string and `context.original_value`.

## End-to-end examples

The repository includes executable examples for `ALWAYS` and `OVERFLOW` modes:

- `examples/filesystem_serdes/filesystem_serdes_always.py`
- `examples/filesystem_serdes/filesystem_serdes_overflow.py`

Run their local end-to-end tests with:

```console
hatch run test:examples -k filesystem_serdes
```

The tests cross a durable wait boundary so completed step results must be
restored from their inline or file-backed checkpoints during replay. They also
inspect the persisted filesystem envelope, preview, digest, and file contents.

Cloud execution is opt-in because the deployed Lambda functions must already
have a durable EFS or S3 Files mount. Set
`FILESYSTEM_SERDES_CLOUD_MOUNT_PATH` to the mounted path before running the
cloud example tests:

```console
FILESYSTEM_SERDES_CLOUD_MOUNT_PATH=/mnt/efs/durable-payloads \
hatch run test:examples-integration -k filesystem_serdes
```

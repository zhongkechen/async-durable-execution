# Python conformance functions

This directory contains the Lambda handlers used with
[`aws/aws-durable-execution-conformance-tests`](https://github.com/aws/aws-durable-execution-conformance-tests).

Generate the per-suite SAM templates from handler metadata:

```bash
hatch run conformance:generate
```

Build a Lambda bundle containing the local SDK, its runtime dependencies, and
all handlers:

```bash
hatch run conformance:build
```

Generated templates are written to `conformance/generated/`. Each function's
leading module docstring or requirement comment supplies its conformance
requirement ID. The deployment workflow generates the templates and bundle,
deploys one CloudFormation stack per suite, and runs the upstream validator for
each suite. Validation requires every current upstream requirement to be
covered and uploads JSON, JUnit XML, and execution-history artifacts.

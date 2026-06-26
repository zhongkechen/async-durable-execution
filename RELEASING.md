# Releasing

This document describes how to cut a release for this monorepo and how the automated PyPI publishing workflow is triggered.

## Packages

This monorepo contains the following packages:

| Package | Path | Tag Prefix |
|---------|------|------------|
| `async-durable-execution` | `async-durable-execution` | `v` |
| `async-durable-execution-runner` | `async-durable-execution-runner` | `v` |
| `async-durable-execution-lambda-layer` | `async-durable-execution-lambda-layer` | `v` |
| `async-durable-execution-examples` | `async-durable-execution-examples` | `v` |

## Versioning

All packages share a single version number defined in the repository root:

- Shared version source: `VERSION.py`

Package metadata reads from `VERSION.py`, so bumping that file updates the SDK, runner, Lambda layer builder, and examples package together.

## Cutting a Release

### 1. Bump the version

Update `__version__` in `VERSION.py`. Commit and merge to `main`.

### 2. Create a GitHub Release

1. Go to the [Releases page](https://github.com/zhongkechen/async-durable-execution/releases) on GitHub.
2. Click **Draft a new release**.
3. Create a new tag following the tagging convention below.
4. Set the release title (typically the same as the tag).
5. Write release notes following the format described in [Release Notes Format](#release-notes-format).
6. Click **Publish release**.

### Tagging Convention

The tag should match the shared monorepo version exactly:

- **All packages:** `v<version>` (for example, `v2.0.0a2`)

Examples:

```text
v2.0.0a2
```

## How Publishing Works

Creating a GitHub Release triggers the [`pypi-publish.yml`](.github/workflows/pypi-publish.yml) workflow automatically. The workflow:

1. **Builds** the SDK and runner packages using [Hatch](https://hatch.pypa.io/) (`hatch build`).
2. **Uploads** the built distributions as artifacts.
3. **Publishes** those packages to [PyPI](https://pypi.org/) using trusted publishing (OIDC-based, no API tokens required).

The workflow runs on the `release: [published]` event, so it fires whenever a release is published on GitHub — no manual intervention is needed beyond creating the release.

> **Note:** The current workflow publishes `async-durable-execution` and `async-durable-execution-runner` to PyPI. The Lambda layer builder and examples package still share the same repo version in `VERSION.py`, but they are not part of the current PyPI publish matrix.

Creating a GitHub Release also triggers the [`lambda-layer-publish.yml`](.github/workflows/lambda-layer-publish.yml) workflow. The workflow:

1. **Builds** a Lambda layer zip from the release tag using the local `async-durable-execution` package.
2. **Publishes** a new Lambda layer version with compatible runtimes `python3.10` through `python3.14`.
3. **Shares** the layer version with principals configured in `LAMBDA_LAYER_SHARE_PRINCIPALS`, or with principals entered in the manual workflow dispatch form.

Set `ACTIONS_LAYER_PUBLISH_ROLE_ARN` to the AWS role used for publishing the layer. The role needs `lambda:PublishLayerVersion` and `lambda:AddLayerVersionPermission` for the target layer. If `ACTIONS_LAYER_PUBLISH_ROLE_ARN` is not set, the workflow falls back to `ACTIONS_INTEGRATION_ROLE_NAME`.

Optional repository variables:

- `LAMBDA_LAYER_AWS_REGION`: AWS Region for publishing. Defaults to `eu-south-1`.
- `LAMBDA_LAYER_NAME`: Lambda layer name. Defaults to `async-durable-execution`.
- `LAMBDA_LAYER_SHARE_PRINCIPALS`: Comma, space, or newline-separated AWS account IDs, AWS organization IDs such as `o-abc123`, or `*` for public sharing.

### Trusted Publisher Configuration

PyPI trusted publishing is configured per project, so `async-durable-execution` and `async-durable-execution-runner` need their own matching publisher entry in PyPI.

For the current workflow, each PyPI project should trust the following GitHub Actions publisher settings:

- Owner: `zhongkechen`
- Repository: `async-durable-execution`
- Workflow file: `.github/workflows/pypi-publish.yml`
- Environment for `async-durable-execution`: `async-durable-execution`
- Environment for `async-durable-execution-runner`: `async-durable-execution-runner`

If PyPI returns `invalid-publisher`, compare the failing job's OIDC claims with the PyPI project settings first. A mismatch in repository name, workflow filename, or environment name is the most common cause.

> **Tip:** The OIDC subject includes the GitHub environment name. If PyPI expects the package-specific environment but the workflow emits a different environment, trusted publishing will fail with `invalid-publisher`.

## Release Notes Format

Release notes should document the monorepo version being released. Use the following structure:

```markdown
## async-durable-execution v2.0.0a2

### Features
- Added support for X
- New `context.foo()` API

### Bug Fixes
- Fixed issue with Y under Z conditions

### Breaking Changes
- Removed deprecated `bar()` method
```

Keep each release note section self-contained so users can follow the release history easily.

## Checklist

Before publishing a release:

- [ ] Version bumped in `VERSION.py`
- [ ] Changes merged to `main`
- [ ] CI checks pass on `main`
- [ ] Release notes written for the version being released
- [ ] Tag follows the naming convention (`vX.Y.Z` or `vX.Y.ZaN`)
- [ ] Trusted publisher exists on both PyPI projects with repository `zhongkechen/async-durable-execution`, workflow `.github/workflows/pypi-publish.yml`, and the package-specific environment name

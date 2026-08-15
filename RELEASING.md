# Releasing

This document describes how to cut a release and how the automated PyPI publishing workflow is triggered.

## Package

This repository publishes the following package:

| Package | Path | Tag Prefix |
|---------|------|------------|
| `async-durable-execution` | `.` | `v` |

## Versioning

The package version is defined in the package metadata module:

- Version source: `async_durable_execution/__about__.py`

Package metadata and repository helper scripts read this version where needed.

## Cutting a Release

### 1. Bump the version

Update `__version__` in `async_durable_execution/__about__.py`. Commit and merge
to `main`.

### 2. Create a GitHub Release

1. Go to the [Releases page](https://github.com/zhongkechen/async-durable-execution/releases) on GitHub.
2. Click **Draft a new release**.
3. Create a new tag following the tagging convention below.
4. Set the release title (typically the same as the tag).
5. Write release notes following the format described in [Release Notes Format](#release-notes-format).
6. Click **Publish release**.

### Tagging Convention

The tag should match the package version exactly:

- **Package:** `v<version>` (for example, `v2.0.0a2`)

Examples:

```text
v2.0.0a2
```

## How Publishing Works

Creating a GitHub Release triggers the [`pypi-publish.yml`](.github/workflows/pypi-publish.yml) workflow automatically. The workflow:

1. **Builds** the SDK package using [Hatch](https://hatch.pypa.io/) (`hatch build`).
2. **Uploads** the built distributions as artifacts.
3. **Publishes** the package to [PyPI](https://pypi.org/) using trusted publishing (OIDC-based, no API tokens required).

The workflow runs on the `release: [published]` event, so it fires whenever a release is published on GitHub — no manual intervention is needed beyond creating the release.

Creating a GitHub Release also triggers the [`lambda-layer-publish.yml`](.github/workflows/lambda-layer-publish.yml) workflow automatically. The workflow:

1. **Builds** two Lambda layer zips from the release tag: the standard layer with the `aioboto` extra and a Botocore-only Python 3.15 preview layer.
2. **Discovers** all enabled commercial and China AWS Regions in the publishing accounts, unless Regions are provided explicitly.
3. **Publishes** the standard layer for compatible runtimes `python3.10` through `python3.14`.
4. **Publishes** the Botocore-only layer for `python3.15` as `<layer-name>-python315-preview`, avoiding native dependencies that do not yet provide Lambda-compatible Python 3.15 wheels.
5. **Shares** each layer version with the account ID configured in the `AWS_ACCOUNT_ID` secret, with the China account ID configured in `AWS_ACCOUNT_ID_CN` for China Regions, with principals entered in the manual workflow dispatch form, or with principals configured in `LAMBDA_LAYER_SHARE_PRINCIPALS`.

Set the repository secret `ACTIONS_LAYER_PUBLISH_ROLE_ARN` to the AWS role used for publishing the layer. The role needs `ec2:DescribeRegions`, `lambda:PublishLayerVersion`, and `lambda:AddLayerVersionPermission` for the target layer. If `ACTIONS_LAYER_PUBLISH_ROLE_ARN` is not set, the workflow falls back to `ACTIONS_INTEGRATION_ROLE_NAME`.
Set the repository secret `AWS_ACCOUNT_ID` to the AWS account ID that should receive `lambda:GetLayerVersion` permission by default.
Set the repository secret `ACTIONS_INTEGRATION_ROLE_NAME_CN` to the AWS China partition role ARN used for publishing in China Regions. The role needs the same `ec2:DescribeRegions`, `lambda:PublishLayerVersion`, and `lambda:AddLayerVersionPermission` permissions in the China account. Set `AWS_ACCOUNT_ID_CN` to the China account ID that should receive `lambda:GetLayerVersion` permission by default in China Regions.

Optional repository variables:

- `LAMBDA_LAYER_AWS_REGIONS`: Comma, space, or newline-separated AWS Regions for publishing. Leave unset to publish to all enabled commercial and China Regions in the publishing accounts. When unset, both the commercial publishing role and `ACTIONS_INTEGRATION_ROLE_NAME_CN` must be configured.
- `LAMBDA_LAYER_NAME`: Stable Lambda layer name, limited to 122 characters so the derived preview name remains within Lambda's 140-character limit. Defaults to `async-durable-execution`; the Python 3.15 preview layer adds the `-python315-preview` suffix.
- `LAMBDA_LAYER_SHARE_PRINCIPALS`: Comma, space, or newline-separated AWS account IDs, AWS organization IDs such as `o-abc123`, or `*` for public sharing in commercial Regions. Used only when neither the manual `share-principals` input nor `AWS_ACCOUNT_ID` is set.

### Trusted Publisher Configuration

PyPI trusted publishing is configured per project, so `async-durable-execution` needs a matching publisher entry in PyPI.

For the current workflow, the PyPI project should trust the following GitHub Actions publisher settings:

- Owner: `zhongkechen`
- Repository: `async-durable-execution`
- Workflow file: `.github/workflows/pypi-publish.yml`
- Environment for `async-durable-execution`: `async-durable-execution`

If PyPI returns `invalid-publisher`, compare the failing job's OIDC claims with the PyPI project settings first. A mismatch in repository name, workflow filename, or environment name is the most common cause.

> **Tip:** The OIDC subject includes the GitHub environment name. If PyPI expects the package-specific environment but the workflow emits a different environment, trusted publishing will fail with `invalid-publisher`.

## Release Notes Format

Release notes should document the package version being released. Use the following structure:

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

- [ ] Version bumped in `async_durable_execution/__about__.py`
- [ ] Changes merged to `main`
- [ ] CI checks pass on `main`
- [ ] Release notes written for the version being released
- [ ] Tag follows the naming convention (`vX.Y.Z` or `vX.Y.ZaN`)
- [ ] Trusted publisher exists on PyPI with repository `zhongkechen/async-durable-execution`, workflow `.github/workflows/pypi-publish.yml`, and environment `async-durable-execution`

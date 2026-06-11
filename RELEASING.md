# Releasing

This document describes how to cut a release for this monorepo and how the automated PyPI publishing workflow is triggered.

## Packages

This monorepo contains the following packages:

| Package | Path | Tag Prefix |
|---------|------|------------|
| `async-durable-execution` | `packages/async-durable-execution` | `v` |
| `async-durable-execution-runner` | `packages/async-durable-execution-runner` | `v` |
| `async-durable-execution-examples` | `packages/async-durable-execution-examples` | `v` |

## Versioning

All packages share a single version number defined in the repository root:

- Shared version source: `VERSION.py`

Package metadata reads from `VERSION.py`, so bumping that file updates the SDK, runner, and examples package together.

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

The tag should match the shared monorepo version:

- **All packages:** `v<version>` (for example, `v2.0.0-a1`)

Examples:

```text
v2.0.0-a1
```

## How Publishing Works

Creating a GitHub Release triggers the [`pypi-publish.yml`](.github/workflows/pypi-publish.yml) workflow automatically. The workflow:

1. **Builds** the SDK and runner packages using [Hatch](https://hatch.pypa.io/) (`hatch build`).
2. **Uploads** the built distributions as artifacts.
3. **Publishes** both packages to [PyPI](https://pypi.org/) using trusted publishing (OIDC-based, no API tokens required).

The workflow runs on the `release: [published]` event, so it fires whenever a release is published on GitHub — no manual intervention is needed beyond creating the release.

> **Note:** The current workflow publishes `async-durable-execution` and `async-durable-execution-runner` to PyPI. The examples package still shares the same repo version in `VERSION.py`, but it is not part of the current publish matrix.

## Release Notes Format

Release notes should document the monorepo version being released. Use the following structure:

```markdown
## async-durable-execution v2.0.0-a1

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
- [ ] Tag follows the naming convention (`vX.Y.Z` or `vX.Y.Z-aN`)

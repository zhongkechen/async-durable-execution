#!/usr/bin/env bash

set -euo pipefail

claude_bin="${GITHUB_ACTION_PATH}/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/claude"
bun_dir="${GITHUB_ACTION_PATH}/bin"

if [[ "${CLAUDE_CODE_SUBPROCESS_ENV_SCRUB:-}" != "1" ]]; then
  echo "::error::Claude subprocess credential scrubbing is not enabled."
  exit 1
fi
if ! command -v bwrap > /dev/null; then
  echo "::error::Claude subprocess PID isolation is unavailable."
  exit 1
fi
# The pinned action uses bubblewrap for Claude tool subprocesses when
# CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1. Verify the target account can actually
# create the PID namespace that keeps those subprocesses from seeing the
# credential-bearing Claude process.
host_pid="$$"
if ! sudo -H -u claude-review -- env HOST_PID="$host_pid" \
  bwrap \
  --unshare-pid \
  --die-with-parent \
  --ro-bind / / \
  --proc /proc \
  --dev-bind /dev /dev \
  /bin/sh -eu -c '
    test ! -e "/proc/${HOST_PID}"
    test -r /proc/self/status
  '
then
  echo "::error::Claude subprocess PID isolation is not functional."
  exit 1
fi
if [[ ! -x "$claude_bin" ]]; then
  echo "::error::The pinned Claude action did not install its bundled Linux CLI."
  exit 1
fi
if [[ ! -x "${bun_dir}/bun" ]]; then
  echo "::error::The pinned Claude action did not expose its Bun executable."
  exit 1
fi
if ! sudo -H -u claude-review -- test -x "$claude_bin"; then
  echo "::error::claude-review cannot execute the pinned Claude CLI."
  exit 1
fi
if ! sudo -H -u claude-review -- test -x "${bun_dir}/bun"; then
  echo "::error::claude-review cannot execute the pinned Bun runtime."
  exit 1
fi

exec sudo -H -u claude-review -- env \
  PATH="${bun_dir}:/usr/local/bin:/usr/bin:/bin" \
  TMPDIR=/home/claude-review/tmp \
  "$claude_bin" "$@"

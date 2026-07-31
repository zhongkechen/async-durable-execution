#!/usr/bin/env bash

set -euo pipefail

context_dir="${GITHUB_WORKSPACE}/.ai-review-context"
if [[ -e "$context_dir" ]]; then
  echo "::error::The trusted base contains the reserved review context path."
  exit 1
fi
mkdir "$context_dir"

verify_current_head() {
  local current_head_sha
  current_head_sha="$(
    gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}" --jq .head.sha
  )"
  if [[ "$current_head_sha" != "$EXPECTED_HEAD_SHA" ]]; then
    echo "::error::The PR changed; review its latest workflow run instead."
    exit 1
  fi
}

verify_current_head

gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}" \
  > "$context_dir/pr.raw.json"
jq \
  --arg base_sha "$EXPECTED_BASE_SHA" \
  --arg head_sha "$EXPECTED_HEAD_SHA" \
  '{
    number,
    title,
    body,
    html_url,
    draft,
    author_association,
    additions,
    deletions,
    changed_files,
    user: .user.login,
    base: {ref: .base.ref, sha: $base_sha},
    head: {ref: .head.ref, sha: $head_sha}
  }' \
  "$context_dir/pr.raw.json" > "$context_dir/pr.json"

# Comparison pages after the first omit GitHub's size-limited files payload.
merge_base_sha="$(
  gh api \
    "repos/${GITHUB_REPOSITORY}/compare/${EXPECTED_BASE_SHA}...${EXPECTED_HEAD_SHA}?per_page=1&page=2" \
    --jq .merge_base_commit.sha
)"
if [[ ! "$merge_base_sha" =~ ^[0-9a-f]{40}$ ]]; then
  echo "::error::GitHub did not return a valid merge-base commit."
  exit 1
fi

# Fetch only the required objects without checking out PR code or persisting
# the job token.
git \
  -c credential.helper= \
  -c credential.helper='!gh auth git-credential' \
  fetch \
  --depth=1 \
  --no-tags \
  --no-recurse-submodules \
  origin \
  "$merge_base_sha" \
  "refs/pull/${PR_NUMBER}/head"

for review_sha in "$merge_base_sha" "$EXPECTED_HEAD_SHA"; do
  if ! git cat-file -e "${review_sha}^{commit}"; then
    echo "::error::GitHub did not return the expected review commits."
    exit 1
  fi
done

git diff \
  --no-ext-diff \
  --no-textconv \
  --find-renames \
  "$merge_base_sha" \
  "$EXPECTED_HEAD_SHA" \
  -- > "$context_dir/pr.diff"

expected_file_count="$(jq -er '.changed_files' "$context_dir/pr.json")"
diff_file_count="$(
  awk '/^diff --git / { count++ } END { print count + 0 }' \
    "$context_dir/pr.diff"
)"
if [[ "$diff_file_count" != "$expected_file_count" ]]; then
  echo "::error::Generated PR diff does not match GitHub's changed file count."
  exit 1
fi

verify_current_head
rm "$context_dir/pr.raw.json"

#!/usr/bin/env bash
# Exercise the release policy against real, isolated Git histories.
set -euo pipefail

checker="$(cd "$(dirname "$0")" && pwd)/release-check.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
checks=0

git init --quiet "$scratch/repo"
cd "$scratch/repo"
git config user.name 'Release check test'
git config user.email 'release-check@example.invalid'
git config commit.gpgsign false
git config tag.gpgsign false
git config core.hooksPath /dev/null
printf '1.3.3\n' > VERSION
git add VERSION
git commit --quiet -m 'Previous release'
git tag -a v1.3.3 -m 'Previous release'
base=$(git rev-parse HEAD)
git commit --quiet --allow-empty -m 'Unreleased change'

expect_pass() {
  local output
  if ! output=$(bash "$checker" "$@" 2>&1); then
    printf 'Expected success, got:\n%s\n' "$output" >&2
    exit 1
  fi
  checks=$((checks + 1))
}

expect_fail() {
  local reason=$1 output
  shift
  if output=$(bash "$checker" "$@" 2>&1); then
    printf 'Expected failure (%s), got:\n%s\n' "$reason" "$output" >&2
    exit 1
  fi
  if [[ $output != *"$reason"* ]]; then
    printf 'Expected failure containing "%s", got:\n%s\n' "$reason" "$output" >&2
    exit 1
  fi
  checks=$((checks + 1))
}

# Unchanged or decreased versions fail against the PR base.
expect_fail 'bump VERSION above 1.3.3' "$base"
printf '1.3.2\n' > VERSION
expect_fail 'bump VERSION above 1.3.3' "$base"

# Both PR and release validation allow explicit patch/minor/major bumps.
for version in 1.3.4 1.3.10 1.4.0 2.0.0; do
  printf '%s\n' "$version" > VERSION
  expect_pass "$base"
  expect_pass
done

for version in '' v1.3.4 1.3 1.3.4.0 01.3.4 1.03.4 1.3.04 1.3.4-rc.1 '1.3.4 ' $'1.3.4\n1.3.5'; do
  printf '%s\n' "$version" > VERSION
  expect_fail 'VERSION must contain MAJOR.MINOR.PATCH' "$base"
done
rm VERSION
expect_fail 'VERSION is missing' "$base"
printf '1.3.4\n' > VERSION
expect_fail 'cannot resolve base' missing-base

# A tag can be newer than the base branch's VERSION (the original failure).
git tag -a v1.3.5 "$base" -m 'Newer release'
expect_fail 'bump VERSION above existing release v1.3.5' "$base"
git tag -d v1.3.5 >/dev/null
git tag -a v2.0.0-rc.1 "$base" -m 'Unsupported prerelease'
expect_fail 'unsupported release tag' "$base"
git tag -d v2.0.0-rc.1 >/dev/null

# The PR base must also be checked when it has not been released yet.
git add VERSION
git commit --quiet -m 'Unreleased version bump'
unreleased_base=$(git rev-parse HEAD)
git commit --quiet --allow-empty -m 'Next PR forgot its bump'
expect_fail 'bump VERSION above 1.3.4' "$unreleased_base"

# A tag owned by another commit cannot be reused.
git tag -a v1.3.4 "$unreleased_base" -m 'Release'
expect_fail 'already belongs to another commit' "$base"
git tag -d v1.3.4 >/dev/null

# Retrying the same annotated release is safe; lightweight tags are rejected.
git tag v1.3.4
expect_fail 'must be annotated'
git tag -d v1.3.4 >/dev/null
git tag -a v1.3.4 -m 'Release'
expect_pass
expect_pass "$base"
expect_fail 'bump VERSION above 1.3.4' "$unreleased_base"

# The shared publisher must not resume a different version's tag at HEAD.
git tag -a v1.3.5 -m 'Wrong version'
expect_fail 'does not match VERSION'

# Incomplete history must not hide earlier versions.
git clone --quiet --depth 1 "file://$scratch/repo" "$scratch/shallow"
cd "$scratch/shallow"
expect_fail 'fetch full history and tags first'

printf 'Passed %d release checks\n' "$checks"

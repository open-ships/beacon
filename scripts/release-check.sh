#!/usr/bin/env bash
# With a base ref, also require a version bump for this PR/push.
# Without one, validate the checked-out commit for release (including reruns).
set -euo pipefail

fail() {
  printf 'release-check: %s\n' "$*" >&2
  exit 1
}

newer_than() {
  awk -v candidate="$1" -v previous="$2" 'BEGIN {
    split(candidate, c, "."); split(previous, p, ".")
    for (i = 1; i <= 3; i++) {
      if (c[i] + 0 > p[i] + 0) exit 0
      if (c[i] + 0 < p[i] + 0) exit 1
    }
    exit 1
  }'
}

[[ $# -le 1 ]] || fail 'usage: release-check.sh [base-ref]'
cd "$(git rev-parse --show-toplevel)"
[[ $(git rev-parse --is-shallow-repository) == false ]] || fail 'fetch full history and tags first'

semver='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
[[ -f VERSION ]] || fail 'VERSION is missing'
version=$(<VERSION)
[[ $version =~ $semver ]] || fail 'VERSION must contain MAJOR.MINOR.PATCH without a leading v or leading zeroes'
tag="v$version"

if [[ -n ${1:-} ]]; then
  base=$(git rev-parse --verify "$1^{commit}") || fail "cannot resolve base $1"
  previous=$(git show "$base:VERSION") || fail "cannot read VERSION at $1"
  [[ $previous =~ $semver ]] || fail "invalid VERSION at $1"
  newer_than "$version" "$previous" || fail "bump VERSION above $previous from $1"
else
  # The shared release workflow resumes tags at HEAD before choosing a new tag.
  while IFS= read -r existing; do
    [[ -z $existing || $existing == "$tag" ]] || fail "tag $existing at HEAD does not match VERSION $version"
  done < <(git tag --points-at HEAD --list 'v*.*.*')
fi

if git show-ref --verify --quiet "refs/tags/$tag"; then
  [[ $(git cat-file -t "refs/tags/$tag") == tag ]] || fail "$tag must be annotated"
  [[ $(git rev-parse "$tag^{commit}") == "$(git rev-parse HEAD)" ]] || fail "$tag already belongs to another commit; bump VERSION"
  printf 'release-check: %s already tags this commit; release can be retried\n' "$tag"
  exit 0
fi

# Match the shared release workflow's tag selection. Reject unsupported tags
# rather than letting its automatic patch fallback choose a different version.
latest=$(git tag --list 'v*.*.*' --sort=-version:refname | sed -n '1p')
if [[ -n $latest ]]; then
  [[ ${latest#v} =~ $semver ]] || fail "unsupported release tag $latest"
  newer_than "$version" "${latest#v}" || fail "bump VERSION above existing release $latest"
fi

printf 'release-check: ready for %s\n' "$tag"

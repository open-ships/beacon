# Release versions

Every PR to `main` must bump `VERSION`, including fixes, dependency updates,
documentation, and CI changes. Do this before opening or updating the PR; do not
wait for the user to request it. One bump per PR is sufficient.

- Fetch `origin/main` and tags before choosing a version. `VERSION` must exceed
  both the PR base's version and every existing release tag.
- Use SemVer: patch for fixes and maintenance, minor for compatible features,
  major for breaking changes. Store only `MAJOR.MINOR.PATCH`, without `v`.
- Run `just release-check` before pushing, along with checks relevant to the
  change. If the release checker changes, run `bash scripts/release-check-test.sh`.
- Include the chosen version and validation in the PR description. Do not create
  a release tag manually: the release workflow publishes the version after CI.

# Distribution

All distributed Beacon binaries, including release archives and container
images, must be built with `CGO_ENABLED=0`. Keep the binary metadata checks in
the release and Docker builds. Fix incompatible dependencies instead of enabling
CGO for distribution; the race detector may still use CGO during testing.

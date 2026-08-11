# Release process

## Cadence

Xret releases when changes are ready, not on a fixed schedule. A release happens when accumulated fixes or features justify a new version on PyPI.

## Version policy

Versions follow [Semantic Versioning](https://semver.org/):

- **patch** (`0.2.1`): bug fixes, no API change
- **minor** (`0.3.0`): new features, backward-compatible API
- **major** (`1.0.0`): breaking API changes

Pre-1.0, minor bumps may include breaking changes.

Each package in the workspace is versioned and released independently. `xret-data` at `0.3.0` does not imply anything about `xret-backtest`'s version.

## Tag convention

```
<package-name>-v<version>
```

- `xret-data-v0.3.0`
- `xret-backtest-v0.1.0`

Pushing a matching tag triggers `.github/workflows/release.yml`, which builds and publishes only that package.

## Cutting a release

```bash
# 1. Bump the version
uv version --bump minor --package xret-data

# 2. Commit and tag
git add -A
git commit -m "release: xret-data v0.3.0"
git tag xret-data-v0.3.0

# 3. Push
git push && git push --tags
```

The release workflow then:

1. Verifies the tag version matches `pyproject.toml`
2. Builds sdist + wheel (`uv build --package`)
3. Smoke-tests the wheel in an isolated venv
4. Publishes to PyPI via Trusted Publishing (OIDC)
5. Creates a GitHub Release with automatically generated release notes and attaches the built sdist and wheel

The build job passes the exact distributions it tested to the publish and GitHub Release jobs as an Actions artifact. If GitHub Release creation fails after PyPI publishing succeeds, rerun only the failed `create-github-release` job so PyPI is not published again.

The workflow does not maintain a repository `CHANGELOG.md`. GitHub Release notes are the release-history surface for the current pre-1.0 stage. Their categories are configured in [`.github/release.yml`](../../.github/release.yml) and are based on pull-request labels. If a release contains no new commits since the previous release, the release step refuses to create a duplicate.

## Adding a new package to the release pipeline

1. Create `packages/xret-<name>/` with its own `pyproject.toml` and independent version
2. Register the PyPI Trusted Publisher for the new project (owner: `nbsp1221`, repo: `xret`, workflow: `release.yml`, environment: `pypi`)
3. Release with `git tag xret-<name>-v0.1.0`

The workflow trigger uses `xret-*-v*`, but each package must have an explicit wheel smoke-test import configured in `.github/workflows/release.yml` before it can be released. Unknown packages fail closed instead of silently testing a different package.

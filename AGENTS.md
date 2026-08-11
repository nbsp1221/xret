# Xret repository guidance

Repository-specific instructions for AI coding agents. Keep changes focused, preserve user work, and support behavioral claims with current evidence.

## Project and layout

- The brand is **Xret**. Use lowercase `xret` only for Python distributions, imports, paths, and identifiers.
- Python 3.12 uv workspace; Polars is the canonical DataFrame library.
- `packages/xret-data/` — `xret-data` distribution.
- `packages/xret-data/src/xret/data/` — `xret.data` import package.
- `packages/xret-data/tests/` — package tests.
- `docs/` — tracked public and contributor documentation.
- `.internal/` — ignored research, drafts, audits, and transient QA evidence.
- `.gjc/` — ignored workflow state and planning/execution evidence.

`xret` is a PEP 420 namespace. Never add `packages/*/src/xret/__init__.py`. Future distributions follow the same mapping, for example `packages/xret-strategy/src/xret/strategy`.

Each distribution keeps its own `LICENSE` copy beside its `pyproject.toml` and declares `license-files = ["LICENSE"]`. `license-files` patterns must stay inside the distribution directory, so a workspace-root path such as `../../LICENSE` is rejected at build time, and `uv_build` includes only what that key references.

## Commands

Use uv. Do not add parallel pip, Poetry, Conda, or requirements-file workflows.

```bash
uv sync --locked --all-packages
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest                    # network tests excluded by project config
uv run pytest -m network         # explicit live-network run
uv build --package xret-data
```

Use focused tests while iterating, then match final verification scope to the claim. Update `uv.lock` through uv, never by hand.

## API and storage invariants

- Use modern Python 3.12 typing and immutable values for domain identities and result contracts.
- Keep I/O explicit:
  - `fetch` uses the remote provider and never changes canonical local state.
  - `sync` reconciles missing coverage and may write canonical state.
  - `scan` reads complete local coverage only and never uses the network.
  - `scan_partial` reads available local data and reports gaps.
- Keep public market identity provider-independent. Do not expose CCXT client IDs or native derivative symbols as the Xret API.
- CCXT is the current provider layer; do not add native exchange REST bypasses.
- Follow established financial bar schemas and semantics. Do not add nonstandard columns merely to restate a standard contract.
- Keep operational provenance in results, file metadata, and catalog state unless row-level provenance has a demonstrated requirement.
- Public APIs expose domain exceptions and chain underlying CCXT, SQLite, and filesystem errors.
- Do not add compatibility aliases, legacy shims, silent fallbacks, or speculative extension points without an approved requirement.
- Canonical market data is Parquet; SQLite is a rebuildable catalog/index.
- Preserve atomic commit, validation, locking, coverage, and recovery behavior.
- Never silently weaken strict scans or accept unexplained gaps, malformed rows, incomplete final bars, duplicate timestamps, or rebuild mismatches.
- Storage and recovery changes require failure-path tests, not only happy paths.

## Testing and verified evidence

Testing has three separate lanes:

1. **Tracked normal-CI tests** — deterministic unit, contract, integration, recovery, and fault-injection coverage; network-independent.
2. **Tracked explicit tests** — heavy E2E, live-network, soak, or qualification tests run manually, on schedule, or at release gates.
3. **Untracked human-style dogfooding** — create a fresh uv project outside the repo, normally under `/tmp`; install the built distribution; use the public API against the real provider; inspect results and adapt the investigation.

Lanes 1 and 2 must be green before Lane 3. Verified promotion is based on Lane 3. Follow [`docs/quality/verified-support.md`](docs/quality/verified-support.md) for the durable public policy and current verified combinations.

Do not turn Lane 3 into another committed harness. Ad hoc scripts, temporary stores, raw output, local paths, debugging history, and detailed QA reports remain outside Git.

## Documentation

Follow [`docs/index.md`](docs/index.md) and the
[documentation policy](docs/development/documentation.md). Git tracks durable
user, contributor, API, architecture, release, and verified-support
documentation. Put research, design drafts, audits, detailed QA reports, and
session-specific evidence under `.internal/`, `.gjc/`, or `/tmp` according to
purpose.

Keep `README.md` concise and user-facing. Do not place transient reports at the
repository root, create empty documentation placeholders, or retain migration
history and old API examples without a real public compatibility obligation.

For GitHub PR, issue, and release descriptions, keep each prose paragraph on one physical line; the 72-character limit applies only to Git commit messages.
PR titles must follow the repository's commit convention because squash merges use them as commit subjects.

## Artifact promotion gate

A file created to investigate, reproduce, benchmark, dogfood, or verify a change is not automatically a repository deliverable. Default it to `/tmp` or `.internal/`.

Before adding any file to a tracked path, all answers must be yes:

- Is it product code, required configuration, durable documentation, or a stable automated regression test?
- Will it remain useful after the current task, machine, session, and debugging context disappear?
- Does it express a maintained contract rather than narrate the investigation?
- Is this its canonical location and format?

If any answer is no, do not promote it into Git.

- Never move an ad hoc dogfooding script into `tests/` merely because it helped verify a change.
- Never commit scratch fixtures, downloaded market data, logs, screenshots, benchmark output, generated reports, or temporary consumer projects.
- Never retain obsolete implementations, commented alternatives, fallback code, or explanatory comments solely as history.
- Extract only durable policy, contracts, or user-facing claims from internal research and QA reports.
- Add a discovered case to tracked tests only when it is deterministic, has stable assertions, and belongs to the maintained test strategy. Rewrite it as a focused regression test; do not copy the exploratory harness.
- Before completion, inspect `git status` and classify every new file as durable-and-tracked or transient-and-ignored.

## Change discipline

- Inspect nearby patterns and authoritative files before editing.
- Treat unexpected changes as user work. Do not revert, stash, delete, commit, or push them unless explicitly requested.
- Fix causes; never suppress tests, warnings, type errors, or quality checks to pass.
- Update affected tests, public exports, examples, reference docs, and verified claims together.
- Prefer the standard library and existing dependencies over new dependencies.
- Do not design for future providers, bar types, storage backends, or compatibility scenarios without a concrete requirement.

## Definition of done

- Requested behavior exists without stubs or fake fallbacks.
- Relevant success, boundary, error, and recovery paths are tested.
- Ruff, formatting, ty, and appropriately scoped pytest checks pass.
- Public packaging or import changes include build and isolated-consumer verification.
- Material live-provider or verified-claim changes include external-user dogfooding.
- Tracked documentation contains only durable current contracts and claims.

## Nested guidance

Add nested `AGENTS.md` only when a subtree gains materially different commands or constraints. The nearest file may specialize this guidance without duplicating it wholesale.

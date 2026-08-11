# Documentation policy

Xret treats public documentation as versioned product code: it lives beside the implementation, is reviewed with behavior changes, and must remain portable across documentation generators.

## Content model

Choose a page type by reader intent:

| Type | Reader need | Content |
|---|---|---|
| Getting started | Learn through a successful first result | A linear, tested walkthrough |
| Guide | Complete a concrete task | Goal, prerequisites, steps, verification |
| Reference | Look up an exact contract | Signatures, fields, schemas, errors, side effects |
| Explanation | Understand the system | Concepts, rationale, architecture, trade-offs |
| Quality | Decide what can be trusted | Stable criteria and verified support claims |
| Development | Contribute or maintain Xret | Setup, tests, docs, releases, governance |

Keep one primary intent per page. Link across types rather than mixing tutorials, design essays, and exhaustive API details into one document.

## Sources of truth

- Public behavior is defined by source, tests, and `docs/reference/` together.
- Docstrings describe individual Python objects and support future generated API pages.
- Handwritten reference pages define cross-object contracts and semantics that generated signatures cannot explain.
- `README.md` is a concise landing page and quick start, not the complete manual.
- `docs/quality/` contains durable public trust claims, not raw QA history.

A behavior change is incomplete until its affected reference, guide, examples, and public support claims agree.

## Do

- Write Markdown under `docs/` and link with repository-relative paths.
- Use stable terminology from the public API.
- Keep examples copyable, minimal, and consistent with tested behavior.
- Keep each prose paragraph on one physical line instead of hard-wrapping it to a fixed column; preserve line breaks required by Markdown structure.
- State side effects, network access, mutation, strictness, and raised errors explicitly.
- Add a page only when maintained content exists.
- Check repository-relative links and examples affected by a documentation change.
- Preserve useful URLs when moving published pages; add redirects when a site has shipped.
- Version published documentation with supported releases when Xret begins maintaining multiple public versions.

## Don't

- Do not publish research notes, audits, planning transcripts, dogfooding scripts, raw logs, local paths, or session reports.
- Do not duplicate the same contract across several pages without a single canonical reference.
- Do not hand-maintain exhaustive symbol listings that can be generated reliably from public docstrings later.
- Do not let examples imply hidden network calls, implicit synchronization, or partial-data acceptance.
- Do not create empty category indexes or speculative documentation sections.
- Do not add a documentation plugin, linter, versioning system, or custom theme until a current requirement justifies its maintenance cost.

## Publishing tooling

The documentation remains generator-neutral Markdown until Xret has a concrete publishing requirement. A site generator, theme, deployment workflow, search integration, API generator, and release version switcher must be selected together against current requirements rather than introduced speculatively.

The content taxonomy and repository-relative links are intended to remain portable. When publishing is required, evaluate current alternatives for Python API generation, accessibility, search, versioning, deployment, and maintenance cost before choosing the toolchain.

## External foundations

This policy follows:

- [Diátaxis](https://diataxis.fr/) for separating tutorials, how-to guides, reference, and explanation;
- [Write the Docs: docs as code](https://www.writethedocs.org/guide/docs-as-code/) for repository review and automation;
- [CommonMark](https://commonmark.org/) for portable Markdown; and
- [Sphinx autodoc guidance](https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html) for the principle that generated API documentation must not introduce import-time side effects.

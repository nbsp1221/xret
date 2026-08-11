# Xret documentation

Quant research for the market, not the backtest.

Xret is an ecosystem for individual quant researchers. Each package owns a focused responsibility within the research workflow and enforces correct practice at that boundary. The first distribution is `xret-data`: explicit, research-grade acquisition and local management of financial market data.

## Start here

- [Getting started](getting-started/index.md) — install `xret-data`, synchronize a market, and read it locally.
- [Roadmap](roadmap.md) — see how Xret plans to connect trusted data, research, backtesting, and eventual live operation.
- [Synchronize and read bars](guides/synchronization.md) — choose between `fetch`, `sync`, `scan`, and `scan_partial`.
- [Consume live bar updates](guides/live-bars.md) — subscribe to typed bar observations and optionally bridge recent history into the live stream.
- [Market data API](reference/api.md) — exact public contracts.
- [Market-data providers](reference/providers.md) — implement historical and optional live capabilities.
- [Verified support](quality/verified-support.md) — combinations exercised against real providers.

## Browse by intent

- **Getting started** teaches a first successful workflow.
- **Guides** solve concrete tasks.
- **Reference** defines exact API, configuration, schema, and error contracts.
- **Explanation** describes concepts, architecture, and design rationale.
- **Quality** records durable public trust criteria and verified combinations.
- **Development** documents current contributor, documentation, and [release](development/releasing.md) policy.

The [roadmap](roadmap.md) is directional. It does not define current behavior or promise release dates; released source, tests, and reference documentation remain the contract.

Directories are added only when they contain maintained content; Xret does not track empty documentation placeholders.

## Documentation governance

Public documentation is maintained as product code in a generator-neutral Markdown structure. See the [documentation policy](development/documentation.md) for content placement, writing rules, and the boundary between durable public documentation and internal evidence.

Research notes, design drafts, audits, raw QA evidence, temporary scripts, downloaded data, and debugging history do not belong in this site. They remain under ignored `.internal/`, `.gjc/`, or external `/tmp` locations according to purpose.

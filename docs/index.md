# Xret documentation

Quant research for the market, not the backtest.

Xret is an ecosystem for individual quant researchers. Each package owns a focused responsibility within the research workflow and enforces correct practice at that boundary. The first distribution is `xret-data`: explicit, research-grade acquisition and local management of financial market data.

## Start here

- [Getting started](getting-started/index.md) — install `xret-data`, synchronize a market, and read it locally.
- [Provider support and trust](explanation/provider-support.md) — understand what “available,” “verified,” “unverified,” and “incompatible” mean before choosing an exchange.
- [Roadmap](roadmap.md) — see how Xret plans to connect trusted data, research, backtesting, and eventual live operation.
- [Synchronize and read bars](guides/synchronization.md) — choose between `fetch`, `sync`, `scan`, and `scan_partial`.
- [Consume live bar updates](guides/live-bars.md) — subscribe to typed bar observations and optionally bridge recent history into the live stream.
- [Canonical time bars](reference/time-bars.md) — exact interval, trade-price, volume, finality, and missing-data meanings.
- [Market data API](reference/api.md) — exact public contracts.
- [Market-data providers](reference/providers.md) — implement historical and optional live capabilities.
- [Verified support](quality/verified-support.md) — combinations exercised against real providers.

## What is available today

| Capability | Current boundary |
|---|---|
| Markets | Crypto spot and perpetual markets representable through the selected provider |
| Historical data | Completed trade OHLCV time bars through `fetch` and `sync` |
| Live data | Transient time-bar updates through providers with live capability |
| Local data | Canonical Parquet storage, strict `scan`, explicit `scan_partial`, validation, and catalog rebuild |
| Built-in provider | CCXT for historical data and CCXT Pro for live bars |

Xret does not use qualification as an allowlist. Provider-advertised scopes may run as `unverified`; all returned observations still pass the same runtime contract. See [Provider support and trust](explanation/provider-support.md) for the complete decision model and [Verified support](quality/verified-support.md) for exact qualification evidence.

## Browse by intent

- **Getting started** teaches a first successful workflow.
- **Guides** solve concrete tasks.
- **Reference** defines exact API, configuration, schema, and error contracts.
- **Explanation** describes concepts, architecture, and design rationale.
- **Quality** records durable public trust criteria and verified combinations.
- **Development** documents current contributor, documentation, [CCXT qualification](development/ccxt-qualification.md), and [release](development/releasing.md) policy.

The [roadmap](roadmap.md) is directional. It does not define current behavior or promise release dates; released source, tests, and reference documentation remain the contract.

Directories are added only when they contain maintained content; Xret does not track empty documentation placeholders.

## Documentation governance

Public documentation is maintained as product code in a generator-neutral Markdown structure. See the [documentation policy](development/documentation.md) for content placement, writing rules, and the boundary between durable public documentation and internal evidence.

Research notes, design drafts, audits, raw QA evidence, temporary scripts, downloaded data, and debugging history do not belong in this site. They remain under ignored `.internal/`, `.gjc/`, or external `/tmp` locations according to purpose.

# Roadmap

Xret is growing toward one trustworthy path from market data to research, backtesting, and eventual paper and live trading. This roadmap describes that direction without promising release dates, version numbers, or final API shapes.

The current contracts are defined by the source, tests, and [reference documentation](reference/index.md). A roadmap item is not available until it appears there and in a released package.

## Product direction

Xret is a family of composable packages rather than one trading application. Each package should remain independently useful and own one clear boundary.

```text
market-data providers → xret-data → research → xret-backtest
                           ↓                       ↓
                           └──→ paper/live runtime ←── execution providers
```

Market-data acquisition and order execution are separate responsibilities. They may integrate with the same venue, but they have different correctness, recovery, authentication, and state-reconciliation contracts.

## Available now

### Trusted historical and live market data

`xret-data` currently provides provider-neutral crypto spot and perpetual time bars with explicit remote and local operations:

- discover current market definitions;
- fetch completed historical bars without changing canonical state;
- synchronize only missing coverage into canonical Parquet;
- read complete local coverage strictly or inspect partial coverage explicitly;
- validate provider results, source lineage, storage, and catalog recovery;
- use the built-in CCXT provider or an experimental external provider contract;
- consume provider-neutral time-bar observations with explicit finality through an async session without mutating historical storage;
- opt into a validated recent snapshot-to-live handoff when initializing a continuous consumer.

See the [market-data API](reference/api.md) and [verified support](quality/verified-support.md) for the maintained contracts and current evidence.

The initial live-bar capability follows these constraints:

- connection and subscription lifecycle must be explicit;
- provider-native symbols and payloads must not become the public API;
- disconnects, queue overflow, and uncertain continuity must not be hidden;
- live delivery must not implicitly mutate canonical historical storage;
- support must remain capability-based, with verified combinations documented separately from theoretically available provider features;
- application servers, user fan-out, authentication, and UI protocols remain outside `xret-data`.

It is deliberately narrower than a generic tick-data framework. Trades, quotes, order books, sequence handling, and high-resolution timestamp semantics will be designed from concrete use cases rather than forced into the bar update contract.

## Exploring next

### Recording and replay

Research eventually needs more than a transient live connection. After live data semantics are proven, Xret can evaluate explicit recording and deterministic replay for the data types whose identity, ordering, precision, and gap contracts are understood.

Recording will be a named operation. Merely subscribing to live data will not silently publish canonical files or claim complete coverage.

## Planned ecosystem

### Reproducible backtesting

`xret-backtest` is planned to consume trusted Xret market data and make strategy experiments reproducible. Its direction includes:

- deterministic simulation inputs and clocks;
- explicit execution and cost assumptions;
- out-of-sample evaluation as a normal workflow;
- experiment provenance and comparable results;
- safeguards against presenting incomplete or in-sample evidence as robust performance.

The backtest package will consume data contracts from `xret-data`; it will not become another market-data downloader or storage implementation.

### Paper and live operation

Paper and live trading are later stages of the same research lifecycle, not shortcuts around it. Their design will build on validated strategies and shared market-data identities while introducing separate execution responsibilities:

- broker or venue order adapters;
- order, fill, account, and position state;
- reconciliation after disconnects and restarts;
- risk controls and staged promotion from paper to live;
- runtime health, recovery, and operator-visible failures.

Order execution will not be added to `xret-data`. A future runtime may compose live data and execution capabilities, but neither side should hide the other's failure or recovery semantics.

### Command-line workflows

`xret-cli` remains planned as an operator surface over maintained package APIs. It should automate explicit workflows such as data synchronization, experiment execution, validation, and operational checks without creating a second set of business rules.

## How this roadmap evolves

Xret follows evidence-driven development:

1. a concrete research or dogfooding need establishes product value;
2. domain and failure semantics are investigated before an interface is fixed;
3. the smallest complete capability is implemented and tested independently;
4. live-provider behavior is qualified separately from deterministic tests;
5. public reference and verified-support claims change only with working code;
6. later stages build on proven contracts rather than speculative extension points.

Priorities may change as real use reveals better boundaries. Completed work is reported in releases and current reference documentation; proposed work remains directional until it passes Xret's implementation and verification gates.

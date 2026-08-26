# Market-data providers

The experimental provider-author API lets an application or separately installed package acquire capability-specific historical market data through Xret without changing Xret itself. CCXT is the default implementation. The packaged `binance-data-vision` provider is a separate historical-OI-only implementation, not a route inside CCXT.

Xret owns canonical identity, finality, schema validation, coverage, storage, locking, and recovery. A provider owns native market resolution, optional market-definition snapshots, historical network observation, and optional live bar delivery. Providers never write Xret's Parquet files or SQLite catalog.

## Architecture

The provider package is organized around a stable provider-independent core and one directory per implementation:

```text
xret/data/providers/
├── __init__.py             # provider-author public exports
├── contracts.py            # immutable SPI values and protocols
├── runtime.py              # trade-bar validation and normalization
├── funding_runtime.py      # settled-funding validation
├── reference_runtime.py    # reference-bar validation
├── oi_runtime.py           # open-interest validation
├── live_runtime.py         # live capability validation and normalization
├── discovery.py            # lazy direct/installed provider binding
├── conformance.py          # public structural conformance validation
├── binance_data_vision/    # packaged historical-OI archive provider
│   ├── __init__.py         # stable provider export and entry-point path
│   └── provider.py         # archive resolution and observation
└── ccxt/                   # built-in general crypto implementation
    ├── provider.py         # capability orchestration
    ├── client.py           # CCXT construction, retry, and transport
    ├── markets.py          # crypto market resolution and identity translation
    ├── capabilities.py     # provider-advertised capability interpretation
    ├── compatibility.py    # exact lossless semantics and window policies
    ├── live.py             # CCXT Pro live-bar session
    ├── semantics.py        # trade-bar canonical-value translation
    └── *_pagination.py     # family-specific observation strategies
```

The Data Vision implementation is a sibling package with only descriptor, `resolve_open_interest_market`, and `observe_open_interest`; it does not fake other capability methods. Separately distributed providers implement the public contract in their own package and use direct injection or the installed-provider entry point. Current identities remain limited to crypto spot and perpetual markets; Xret does not claim that non-crypto identity or session semantics have been designed.

## Public provider API

Provider authors import from `xret.data.providers`:

```python
from xret.data.providers import (
    PROVIDER_API_VERSION,
    PROVIDER_BAR_SCHEMA,
    PROVIDER_FUNDING_SCHEMA,
    PROVIDER_OPEN_INTEREST_SCHEMA,
    PROVIDER_REFERENCE_BAR_SCHEMA,
    BarObservation,
    BarRequest,
    DerivativeInterpretation,
    FundingObservation,
    FundingRequest,
    HistoricalBarProvider,
    HistoricalFundingProvider,
    HistoricalOpenInterestProvider,
    HistoricalReferenceBarProvider,
    LiveBarProvider,
    LiveBarSession,
    Market,
    MarketDefinition,
    MarketDefinitionProvider,
    MarketIdentity,
    ObservedWindow,
    OpenInterestObservation,
    OpenInterestRequest,
    OpenInterestSourceEvidence,
    ProviderDescriptor,
    ProviderBarUpdate,
    ReferenceBarObservation,
    ReferenceBarRequest,
    ResolvedBarMarket,
    ResolvedFundingMarket,
    ResolvedOpenInterestMarket,
    ResolvedReferenceMarket,
    OpenInterestSyncPolicy,
    validate_provider_conformance,
)
```

This namespace is self-contained for provider authoring; provider packages do not import domain values from implementation modules such as `xret.data.models`.

The provider SPI is structural and capability-specific. A descriptor plus at least one complete recognized resolve/observe pair is required; historical bars, settled funding, reference bars, and historical OI are independent capabilities. Supplying only one method of a pair is a provider contract error. Live bars and market definitions are optional. The SPI does not expose raw trades, quotes, order books, fundamentals, provider-specific row columns, fallback, or synthetic timeframes.

Provider value frames omit canonical identity, which Xret attaches after validation:

| Capability pair | Provider schema | Time meaning |
|---|---|---|
| `resolve_market` / `observe_bars` | `timestamp, open, high, low, close, volume` | Aligned completed executed-trade intervals; volume is base quantity. |
| `resolve_funding_market` / `observe_funding` | `effective_at, funding_rate, funding_interval_seconds?, mark_price?` | Final public settlement events in arbitrary UTC half-open ranges. |
| `resolve_reference_market` / `observe_reference_bars` | `timestamp, open, high, low, close` | Aligned completed mark, index, or premium-index intervals; no volume. |
| `resolve_open_interest_market` / `observe_open_interest` | `timestamp, open_interest_amount, open_interest_value?` | Provider-labeled samples on the requested grid; amount is base-asset-equivalent exposure and value is quote notional. |

Each observation returns ordered, non-overlapping `ObservedWindow` evidence. Absence has meaning only inside those windows; providers may return a proved subset of the request for funding, reference, and OI. Xret preserves every unproved remainder as `missing`. `FundingObservation.normalizations` carries transformations performed by that exact call, such as identical-duplicate removal; providers must not communicate observation evidence through shared mutable state or private runtime hooks.

`HistoricalBarProvider` is a structural protocol. Inheritance is optional; an implementation supplies:

```python
class HistoricalBarProvider(Protocol):
    @property
    def descriptor(self) -> ProviderDescriptor: ...

    def resolve_market(self, identity: MarketIdentity) -> ResolvedBarMarket: ...

    def observe_bars(
        self,
        request: BarRequest,
        market: ResolvedBarMarket,
    ) -> BarObservation: ...
```

## Optional market-definition capability

Market-definition discovery is a separate structural protocol; adding it does not change `HistoricalBarProvider` SPI v1 or require existing providers to implement it.

```python
class MarketDefinitionProvider(Protocol):
    def fetch_markets(
        self,
        *,
        exchange: str,
        market: Market,
    ) -> tuple[MarketDefinition, ...]: ...
```

A provider used through `MarketData` supplies a descriptor and at least one complete recognized resolve/observe capability pair. Historical bars are not mandatory: the packaged Data Vision provider is deliberately OI-only. A provider may additionally implement `MarketDefinitionProvider`. Calling `MarketData.fetch_markets(...)` against a provider without this optional capability raises `UnsupportedMarketError`; Xret never falls back to CCXT after an explicitly selected provider lacks or fails the operation.

Every returned definition must belong to the requested canonical exchange and market family, and canonical identities must be unique. Xret rejects mutable collections, wrong value types, out-of-scope definitions, and duplicate identities as provider contract failures.

`MarketDefinition` is immutable and contains canonical identity, nullable provider-advertised active status, canonical provider-advertised trade-bar timeframes, optional exact `tick_size` and `size_increment`, and optional derivative interpretation. `bar_capabilities` reports historical/live trade bars; `funding_history` reports settled funding; `reference_bar_capabilities` reports exact kind/timeframe pairs; and `open_interest_capabilities` reports OI timeframes. These values describe current provider-advertised operability and known compatibility, not exhaustive pagination or Xret qualification. Search, filtering, ordering, and result caching remain application responsibilities.

The built-in CCXT adapter translates only entries safely expressible with the requested spot or perpetual identity. Unrelated native instrument families, unknown optional fields, and native timeframe names outside Xret's grammar do not reject the venue. Canonical identity collisions are excluded rather than resolved by exposing or arbitrarily selecting a provider-native symbol. CCXT precision values become increments only in `TICK_SIZE` mode; limits and other precision modes are not guessed into fixed increments.

## Optional live-bar capability

Adding live bars does not change `HistoricalBarProvider` SPI version 1 or force existing providers to implement streaming. A provider may additionally expose:

```python
class LiveBarProvider(Protocol):
    def open_live_bars(self, *, exchange: str) -> LiveBarSession: ...
```

`LiveBarSession` is an async context manager and async iterator. Its `subscribe_bar_updates(resolved_market, timeframe)` method starts one stream; its iterator merges `ProviderBarUpdate` values from every subscription. The provider update carries canonical identity, timeframe, inclusive UTC bar-start, trade-derived OHLC values, and base-asset volume. Xret validates it, enforces per-dataset nondecreasing timestamps, and adds `received_at` before exposing `BarUpdate`. Xret also derives provider-neutral `BarFinality` from the bar interval, receipt time, and Xret's finality grace; providers do not add native closed/confirm flags to the SPI.

The built-in provider implements this capability through CCXT Pro with `newUpdates=True` and rate limiting enabled. Async clients are distinct from historical sync clients and are reused by native CCXT client ID within one session. A canonical Binance session may therefore own separate `binance` and `binanceusdm` clients for spot and USD-M perpetual subscriptions. Xret attempts an Xret-expressible timeframe when CCXT Pro advertises `watchOHLCV`; the published live matrix supplies confidence evidence rather than permission. Historical REST and live WebSocket adapters may expose different native volume fields, so the live channel inherits the historical typed volume policy by default and requires an explicit channel override when qualification proves a difference. Scopes without prior qualification still run under the same compatibility rules and runtime validation, while malformed or known non-lossless values fail. A field name or adapter comment is not sufficient semantic evidence: qualification compares a fully observed public-trade interval with the completed live candle and exact instrument metadata before promoting a public base-quantity or contract-count claim.

Live capability absence raises `UnsupportedMarketError`. Once open, a provider transport failure, malformed update, reader failure, or queue overflow raises a terminal `ProviderError` for the session. Providers and Xret do not silently retry, reconnect, coalesce, or claim continuity. Closing the context closes the provider's whole session; the initial contract has no unsubscribe operation.

The optional public bootstrap reuses the mandatory historical `observe_bars` operation only for a bounded range of already closed intervals. Xret validates that recent observation without applying canonical-storage finality, merges it in memory with buffered live updates, and never writes provider observations to storage. Provider authors do not implement a second snapshot SPI.

## Descriptor and market resolution

`ProviderDescriptor` contains:

- `name`: lowercase stable source-lineage slug;
- `version`: nonempty audit string, not required to follow PEP 440;
- `api_version`: exact provider SPI major implemented by the provider.

The provider name identifies the implementation lineage, not the exchange. For example, a Coinbase-native implementation might be named `coinbase-advanced` while resolving the canonical venue `coinbase`.

`resolve_market` receives Xret's provider-independent `MarketIdentity`. It returns the same canonical identity, provider-native IDs used for provenance, and the timeframes supported for that resolved market. A provider may resolve an omitted perpetual settlement only when it can do so unambiguously. It must not relabel the canonical venue, symbol, or market family.

`timeframes` declares what Xret may request from that market, so every entry must be a canonical Xret timeframe whose interval origin, event universe, volume unit, finality, and exhaustive-observation behavior the provider can satisfy. A venue legitimately offers bar types outside that vocabulary; exclude them instead of passing them through. A venue must not become unresolvable because it offers a bar type Xret cannot express or normalize losslessly.

Excluding an entry is not a silent fallback. Requesting a non-canonical timeframe raises `InvalidRequestError` before any provider call, because the timeframe grammar rejects it. Requesting a canonical timeframe this venue does not offer raises `UnsupportedMarketError`. Neither case substitutes another bar type.

## Observation contract

`observe_bars` receives a UTC-aware, aligned, half-open `BarRequest`. It returns:

- a Polars `DataFrame` with exactly `PROVIDER_BAR_SCHEMA`;
- one or more `ObservedWindow` values proving where absence is meaningful.

The provider frame contains only:

```text
timestamp, open, high, low, close, volume
```

Identity columns are deliberately absent. Xret adds canonical identity after validating the returned value frame. OHLC values must summarize eligible executed trades and `volume` must already represent base-asset quantity. A provider must reject a source when mark/index/settlement prices or quote/contract volume cannot be normalized exactly. An empty frame is valid when the provider exhaustively observed the requested window.

These meanings make explicit the canonical semantics already required by the provider value schema; they do not add a method or field to SPI version 1. A provider that emitted a differently defined value was not producing canonical Xret OHLCV even if its frame shape passed structural validation.

Observation evidence is stronger than returned rows. In the current SPI major, observed windows must be ordered, non-overlapping, inside the request, and aligned to a grid when the family has one. They may prove only a subset of the request; every returned row must lie inside an observed window, and every unproved remainder stays `missing`. Immediately before calling the provider, Xret records a conservative evidence time and records completion separately after the call returns. For finalized bar families, the completed-row gate and negative coverage use only the pre-call evidence time, so an interval becoming final during a slow request remains `missing` for the next sync. Xret also rejects rows outside the request or evidence and enforces the applicable family invariants.

This distinction prevents a temporary empty native page from turning an unqueried tail into false `unavailable` coverage:

```text
no returned row != proof that the entire remaining range was observed empty
```

For an endpoint with a maintained bounded-window policy, the built-in CCXT adapter partitions a request into exact windows and sends the actual number of remaining bar boundaries on the final page, not the endpoint's maximum page size. A typed endpoint profile may omit CCXT's unified `until` when the adapter derives the native end from `since + limit`. A profile may also recognize a documented closed native window by accepting only the exact right-boundary candle as an observation witness and discarding it from Xret's half-open result.

An endpoint without an exact policy is not blocked. Xret uses CCXT's advertised page limit when available, advances only from validated returned timestamps, rejects ignored `since`, backward data, conflicting overlap, malformed rows, and non-progress, and stops under a deterministic request budget. This generic strategy establishes presence only: returned bar intervals are observed, while every other interval remains `missing` and retryable. It never turns an unknown empty response into unavailable coverage.

## Implement, validate, and connect a provider

Implement one class with a `ProviderDescriptor` and at least one complete capability pair from the table above. Keep resolution I/O-free when the provider can derive native identity from supplied configuration; observation methods perform remote reads and return only the matching immutable contract values. OI providers use incremental missing-range synchronization by default. A revisioned archive declares `open_interest_sync_policy = OpenInterestSyncPolicy.REVISIONED_ARCHIVE` on the provider and returns the same policy from `ResolvedOpenInterestMarket.sync_policy`; Xret validates the declaration without I/O and rejects a resolved-market mismatch. Use this policy only when the source can revise certified ranges and every observation supplies contributor evidence suitable for replacing the provider-owned range. Return call-specific provenance through `OpenInterestObservation.normalizations` and `source_field_mapping`, not private methods.

Validate the finished object before integration:

```python
from xret.data.providers import validate_provider_conformance

provider = AcmeProvider(...)
descriptor = validate_provider_conformance(provider)
```

This validator performs no market resolution, network access, or storage I/O. It validates the descriptor, accepts any complete recognized capability pairs, rejects partial pairs, and rejects an object with no capability. Use that same validated object directly with `MarketData(provider=provider)`. If the provider can be constructed without application-owned arguments, expose a zero-argument factory through the `xret.data.providers` entry-point group and select its descriptor name with `MarketData(provider="acme")`. Direct injection and installed discovery apply the same conformance rules; neither path registers fallback providers.

## Direct injection

Direct injection is the primary integration path. It supports application-owned credentials, sessions, clients, and lifecycle without a global registry:

```python
from xret.data import MarketData

provider = MyHistoricalBarProvider(...)
market_data = MarketData(provider=provider)
```

Construction and `bars(...)` do not inspect the provider. `scan`, `scan_partial`, `maintenance.validate`, and `maintenance.rebuild_catalog` remain local-only and never resolve it.

## Installed provider packages

A distribution may expose a zero-argument factory through the `xret.data.providers` entry-point group:

```toml
[project.entry-points."xret.data.providers"]
acme = "acme_xret:create_provider"
```

```python
def create_provider():
    return AcmeProvider(...)
```

Consumers select that exact name:

```python
market_data = MarketData(provider="acme")
```

Discovery, import, and factory execution are lazy and cached per `MarketData` instance. Unknown or duplicate names, import or factory failures, descriptor-name mismatch, incompatible API versions, and missing protocol methods raise `ProviderError`. Xret never falls back to CCXT after an explicitly selected provider fails.

A zero-argument factory is suitable for providers configured from their own environment or configuration files. Providers needing application-owned runtime objects should use direct injection.

## Packaged Binance Data Vision provider

Select the packaged archive provider explicitly:

```python
from xret.data import MarketData

archive = MarketData(provider="binance-data-vision")
```

It implements only `resolve_open_interest_market` and `observe_open_interest`. Its admitted scope is Binance `BTC/USDT`, USDT-settled linear perpetual, `5m`, using official USDⓈ-M daily metrics archives and whole UTC-day requests of 1 through 366 days. It has no trade bars, funding, reference bars, live session, market discovery, CCXT delegation, or Binance REST fallback. Selecting it for another operation or identity raises rather than switching providers.

For each day Xret verifies the official SHA-256 sidecar and filename, bounded ZIP structure, sole expected CSV member, exact header, symbol, UTC day/grid, finite nonnegative Decimal values, ordering, and duplicates. Exact duplicates are deduplicated; conflicts fail. Missing objects and absent slots remain `missing` and carry publication-lag evidence. Amount maps from `sum_open_interest`; value maps from `sum_open_interest_value`.

Data Vision re-observes requested archive days so a changed checksum can replace only the same certified owned interval, including deleting rows that disappeared from the revised object. OI canonical files carry ordered, non-overlapping contributor manifests, and catalog v6 indexes provider ownership. Another provider may own a disjoint recent range in the same OI dataset, but cannot synchronize an overlapping Data Vision range. Cross-provider overlap is a fetch-only qualification operation and never chooses or publishes a winner.

## Source lineage and recovery

For trade bars, settled funding, and reference bars, the first `sync` that commits provider-derived canonical facts binds a canonical dataset to `ProviderDescriptor.name`. Those facts may be available rows or unavailable coverage for a finalized range. A successful remote observation that produces neither does not bind lineage. A newer implementation version with the same name may continue the history. A different name cannot silently append or rewrite it.

Open interest additionally supports contributor-managed artifacts. Each contributed half-open range has one immutable provider owner, contributor ranges must be ordered and non-overlapping, and disjoint providers may coexist in one canonical monthly file. Explicit contributor ranges must cover every observed window and returned row before they can become ownership state. They may additionally cover a certified source-revision interval with absent samples, which is required to represent sparse or zero-row archive revisions without claiming those samples as available. Provider ownership is not row-level selection or fallback: an overlapping sync by the wrong provider fails before observation or publication.

This is a lineage constraint, not part of canonical market identity: `exchange`, `symbol`, `market`, `settle`, and `timeframe` still identify the dataset. Provider name, version, API version, native market ID, and native symbol are operational provenance stored in canonical Parquet metadata and ingestion state. Parquet metadata describes the provider snapshot that most recently published the current physical monthly file; it is not row-level acquisition history for every bar merged into that file. While the catalog exists, ingestion runs retain the provider snapshot for each remote operation. Rebuild can recover only the latest file snapshot and the stable provider-name lineage from canonical Parquet. Provider version and native IDs are audit facts, not lineage equality fields; changes under the same provider name update the latest publication snapshot and remain visible in ingestion runs while the catalog exists.

Available lineage can be rebuilt from ordinary canonical Parquet. OI contributor ownership and evidence can be rebuilt from contributor manifests. When a certified OI revision removes every row in a month, Xret retains a zero-row canonical OI artifact containing only the validated contributor manifest; it proves immutable source ownership across catalog rebuilds but proves no sample availability. An exhaustive empty observation can create unavailable coverage and ordinary lineage without producing a Parquet file; those catalog-only facts are intentionally non-rebuildable and return to `missing` with no source binding. Funding event files rebuild without completeness because irregular rows do not prove the surrounding observed span.

## Errors and external qualification

Provider-native exceptions should be allowed to propagate from provider methods; Xret wraps unknown failures in `ProviderError` and chains the original cause. Providers should use `UnsupportedMarketError` when a requested market or timeframe cannot be operated safely.

Conformance to these protocols means Xret can validate and orchestrate the implementation. It does not make a third-party provider a previously qualified source. Historical QA claims remain separate public evidence under [Verified support](../quality/verified-support.md) and are not part of the provider protocol or runtime result.

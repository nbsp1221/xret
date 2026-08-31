# Market data API

## Package exports

`from xret.data import ...` exposes exactly:

- `MarketData`
- `MarketDataConfig`
- `BarDataset`
- `SettledFundingDataset`
- `FundingFetchResult`
- `FundingSyncResult`
- `FundingPartialScanResult`
- `ReferenceBarDataset`
- `ReferenceBarFetchResult`
- `ReferenceBarSyncResult`
- `ReferenceBarPartialScanResult`
- `OpenInterestDataset`
- `OpenInterestFetchResult`
- `OpenInterestSyncResult`
- `OpenInterestPartialScanResult`
- `ReferencePriceKind`
- `ReferenceBarKey`
- `ReferenceBarCapability`
- `DatasetFamily`
- `SettledFundingKey`
- `OpenInterestKey`
- `BarUpdate`
- `BarFetchMode`
- `BarFinality`
- `Availability`
- `CapabilityNotice`
- `OperationCapability`
- `TimeBarCapability`
- `ProviderEvidence`
- `FetchResult`
- `LiveSubscription`
- `LiveMarketData`
- `SyncResult`
- `PartialScanResult`

## `MarketData`

```python
MarketData(
    config: MarketDataConfig | None = None,
    *,
    provider: object | None = None,
)
```

Construction resolves configuration but performs no provider or storage I/O. An explicit config bypasses configuration discovery. Omitting `provider` selects the built-in CCXT provider. A provider object uses direct dependency injection; a string selects an installed provider entry point. Both forms remain unresolved until `fetch_markets`, a live context, `fetch`, or `sync` needs the provider. See [Market-data providers](providers.md).

### `fetch_markets`

```python
market_data.fetch_markets(
    *,
    exchange: str,
    market: str,
) -> tuple[MarketDefinition, ...]
```

Fetches the selected provider's current market definitions for one canonical venue and market-family scope. This is an eager remote metadata operation. It never reads or changes canonical Parquet, the SQLite catalog, coverage, locks, or source lineage, and it does not cache the returned snapshot.

```python
from xret.data import MarketData

market_data = MarketData()
definitions = market_data.fetch_markets(
    exchange="binance",
    market="perpetual",
)

active_markets = [
    definition.identity
    for definition in definitions
    if definition.active is not False
]
```

Applications that need to name the result type in annotations import `MarketDefinition` from the deliberate public provider-contract namespace:

```python
from xret.data.providers import MarketDefinition
```

`exchange` is a lowercase canonical slug and `market` is `spot` or `perpetual`. They define the provider endpoint and normalization scope; they are not search filters. The method deliberately has no query, symbol, settlement, active-status, sorting, or pagination parameters. Callers search, filter, sort, and cache the returned immutable tuple themselves.

Each `MarketDefinition` contains:

- `identity`: provider-independent `MarketIdentity`, including settlement for perpetuals;
- `active`: provider-advertised `True` or `False`, or `None` when unknown;
- `timeframes`: provider-advertised timeframe names that Xret's canonical grammar can express;
- `tick_size`: a positive exact `Decimal` fixed price increment, or `None`;
- `size_increment`: a positive exact `Decimal` fixed quantity increment, or `None`;
- `derivative`: linear/inverse and contract-size interpretation for a perpetual, otherwise `None`;
- `bar_capabilities`: per-timeframe historical and live trade-bar availability plus capability notices;
- `funding_history`: settled-funding availability and notices;
- `reference_bar_capabilities`: availability and notices for each mark, index, or premium-index kind/timeframe; and
- `open_interest_capabilities`: per-timeframe historical OI availability and notices.

`active=True` is not a guarantee that every venue operation is currently available. `timeframes` remains the concise trade-bar catalog and is not an exhaustive-pagination or qualification claim. Inspect the family-specific capability fields to distinguish `available`, `unavailable`, and `incompatible`. Xret attempts available operations and validates the current response.

These fields answer discovery-time questions, not whether a later network request succeeded. See [Provider support and trust](../explanation/provider-support.md) for the state model.

```python
from xret.data import Availability

definitions = market_data.fetch_markets(exchange="binance", market="spot")
one_minute = next(
    capability
    for capability in definitions[0].bar_capabilities
    if capability.timeframe == "1m"
)
can_fetch = one_minute.historical.availability is Availability.AVAILABLE
notices = one_minute.historical.notices
```

Provider-native client IDs, derivative symbols, and raw metadata are not part of `MarketDefinition`. Native transport failures raise chained `ProviderError`; a selected provider without the optional market-definition capability raises `UnsupportedMarketError`. A successful empty tuple means the provider returned no safely representable market in the requested scope.

### `bars`

```python
market_data.bars(
    *,
    exchange: str,
    symbol: str,
    market: str,
    settle: str | None = None,
    timeframe: str,
) -> BarDataset
```

Binds one provider-independent dataset identity and performs no I/O.

- `exchange` is a lowercase canonical slug, such as `binance`.
- `symbol` is an NFC-normalized `BASE/QUOTE` pair with exactly one structural `/` and nonempty Unicode components, such as `BTC/USDT`.
- `market` is `spot` or `perpetual`.
- Spot omits `settle`. A resolved perpetual identity always has a nonempty `settle` component without `/`. For `fetch`/`sync`, an omitted perpetual `settle` is inferred only when provider metadata has exactly one nonempty settlement value and exactly one listed perpetual market matching the base/quote and that settlement. Local reads infer an omitted `settle` only from exactly one locally known dataset candidate.
- `timeframe` is case-sensitive `<amount><unit>`. Units are `s`, `m`, `h`, `d`, `w`, and `M`; `w` and `M` require amount `1`.

All historical and live rows use Xret's [canonical trade time-bar contract](time-bars.md). OHLC summarizes eligible executed trades, `volume` is base-asset quantity, and a provider-native timeframe is usable only when its complete interval semantics match or can be normalized losslessly. Fixed `s`, `m`, `h`, and `d` multiples use the Unix epoch as their origin; `1w` begins Monday UTC and `1M` begins at the UTC calendar-month boundary. Consequently `7d` and `1w` are distinct identities.

### `settled_funding`

```python
market_data.settled_funding(
    *,
    exchange: str,
    symbol: str,
    market: str,
    settle: str | None = None,
) -> SettledFundingDataset
```

Binds one provider-independent series of final public funding events and performs no I/O. `market` must be `perpetual`; settlement inference follows the same single-candidate remote/local rules as perpetual trade bars. The family deliberately excludes current or predicted rates, private account payments, and funding PnL calculations.

Canonical rows contain `exchange, symbol, market, settle, effective_at, funding_rate, funding_interval_seconds, mark_price`. `effective_at` preserves the provider's exact UTC millisecond event time rather than rounding to a nominal schedule. `funding_rate` is finite. `funding_interval_seconds` is nullable and positive when present; `mark_price` is nullable and positive finite when present. Xret does not assume a fixed eight-hour schedule or infer an interval from neighboring events.

Funding accepts arbitrary UTC-aware half-open ranges because it is an irregular event series. Only events strictly before the conservative pre-call evidence instant are accepted, and observed spans are clipped at that instant; a requested future tail therefore remains `missing` for a later sync. `fetch`, `sync`, `scan`, and `scan_partial` keep the standard remote-only, reconcile, strict-local, and explicit-partial boundaries and return funding-specific result types. Provider-observed spans, not event spacing, establish completeness. A rebuild can recover event files but cannot prove the surrounding observation spans, so rebuilt funding coverage is empty until those ranges are synchronized again.

### `reference_bars`

```python
market_data.reference_bars(
    *,
    exchange: str,
    symbol: str,
    market: str,
    settle: str | None = None,
    kind: str,
    timeframe: str,
) -> ReferenceBarDataset
```

Binds one provider-independent historical reference series and performs no I/O. `market` must be `perpetual`; settlement inference follows the same single-candidate remote/local rules as perpetual trade bars. `kind` is exactly `mark`, `index`, or `premium_index`, and each kind/timeframe has independent identity, path, lock, coverage, and source lineage.

A reference row has `exchange, symbol, market, settle, timeframe, timestamp, open, high, low, close`. It never has `volume`: native placeholder volume and unrelated kline fields are discarded at the provider adapter. Mark and index OHLC values are finite and positive. Premium index is a dimensionless dislocation series, not a currency price, so its finite OHLC values may be negative or zero while preserving `low <= open, close <= high`.

`fetch`, `sync`, `scan`, and `scan_partial` have the same remote-only, reconcile, strict-local, and explicit-partial I/O boundaries as `BarDataset`, but return reference-family result types. Requests must align to the timeframe grid. Only time-closed rows beyond Xret's finality grace are accepted; no absent value is synthesized. A missing slot in an exhaustively observed grid is `unavailable`, so strict scans reject it. Pair-scoped index routes remain stored under the requested derivative identity while provider evidence and Parquet provenance identify `reference_target_scope="pair"` and the native pair.

### `live`

```python
market_data.live(*, exchange: str) -> LiveMarketData
```

Binds a one-shot asynchronous live session for one canonical exchange and performs no I/O. Enter the returned context before subscribing:

```python
async with market_data.live(exchange="binance") as live:
    receipt = await live.subscribe_bar_updates(bars)
    print(receipt.source, receipt.warnings)
    async for update in live:
        ...
```

`bars` must be a `BarDataset` created by the same `MarketData` instance and must use the session exchange. One session may merge several bar subscriptions into its single-consumer iterator. The only current event type is immutable `BarUpdate`, containing canonical identity, timeframe, inclusive UTC bar-start timestamp, trade-derived OHLC floats, base-asset `volume`, Xret's UTC normalization receipt time, and `BarFinality` (`FORMING`, `PROVISIONAL`, or `FINAL`). Finality describes the observation relative to the bar interval and Xret's finality grace. It never claims that the value is stored as canonical data.

### `open_interest`

```python
market_data.open_interest(
    *,
    exchange: str,
    symbol: str,
    market: str,
    settle: str | None = None,
    timeframe: str,
) -> OpenInterestDataset
```

Binds one provider-independent historical open-interest series and performs no I/O. `market` must be `perpetual`; settlement inference follows the same single-candidate remote/local rule as other perpetual families. Open interest is a non-directional point-in-time gauge sampled at the provider-labeled row `timestamp`. It is not interval flow, turnover, or evidence of long/short direction, and Xret never forward-fills a missing sample.

Canonical rows contain `exchange, symbol, market, settle, timeframe, timestamp, open_interest_amount, open_interest_value`. Amount is finite nonnegative base-asset outstanding exposure. The built-in CCXT adapter admits only unambiguously linear, non-inverse contracts with an exact positive contract size, consumes CCXT's unified `openInterestAmount` directly, and converts its Decimal representation once to Float64. Contract size remains interpretation and provenance metadata; it is not applied again to the unified amount. Value is nullable finite nonnegative quote-currency notional. Signed zero is normalized to positive zero; inverse, quanto, ambiguous, nonfinite, and overflowing values fail before return or storage.

Requests use aligned half-open sample-grid ranges. `fetch`, `sync`, `scan`, and `scan_partial` preserve the standard remote-only, reconcile, strict-local, and explicit-partial boundaries and return OI-specific results. Each expected timestamp is its own completeness unit. The CCXT route proves returned samples only: sparse omissions and ranges outside source retention remain `missing`, never `unavailable`, and no current snapshot call patches historical data. Result and Parquet provenance record the derivative interpretation, contract size, native field mapping, and named conversion.

The separately selected installed provider `MarketData(provider="binance-data-vision")` supports official Binance Data Vision USDⓈ-M perpetual daily metrics archives at `5m`; it has no bars, funding, reference, live, discovery, CCXT delegation, or Binance REST fallback. Requests must use whole UTC days. Xret verifies the official SHA-256 sidecar and object identity, bounds and validates the ZIP and sole CSV member, parses the exact schema/day/grid with Decimal-to-Float64 and positive-zero normalization, stable-sorts rows, deduplicates exact duplicates, and rejects conflicting duplicates. A 404 or missing slot remains `missing` with publication-lag evidence. `sources` records each contributed range, object key, checksum/revision, retrieval time, row counts, route, and normalizations.

Canonical OI artifacts carry ordered, non-overlapping contributor ranges. The catalog separately indexes immutable provider ownership, so a sync selected with the wrong provider fails before publication even when the range is already covered. Data Vision re-observes requested archive days to detect checksum changes; a changed revision set-replaces rows and coverage only inside the same owned certified interval, including deletions, while preserving disjoint contributors. Cross-provider overlap is fetch-only qualification evidence and differing same-key canonical values fail without choosing a winner.

`subscribe_bar_updates(bars) -> LiveSubscription` starts live-only delivery and returns the resolved dataset identity, provider evidence, normalization identifiers, and initial warnings after the provider session's subscription method completes. The receipt does not claim a provider-level acknowledgement, first event, historical/live continuity, or canonical coverage. The operation never performs historical observation or reads or changes canonical storage.

Same-timestamp updates are valid. Backward timestamps, provider failures, malformed events, and bounded queue overflow fail the whole session with `ProviderError`; Xret does not silently retry, reconnect, or drop old events. Ordering is nondecreasing per dataset, not globally across different datasets in one session. See [Consume live bar updates](../guides/live-bars.md) for lifecycle and continuity guidance.

## Time ranges

Every data verb requires `start` and accepts optional `end`. Inputs may be timezone-aware `datetime` values, ISO dates, or offset-bearing ISO timestamps. Naive datetimes are rejected and all ranges are UTC-aware and half-open (`[start, end)`). Trade bars, reference bars, and OI require bounds aligned to their timeframe; settled funding accepts arbitrary UTC-aware bounds.

## `BarDataset.fetch`

```python
fetch(start, end=None, *, mode: BarFetchMode) -> FetchResult
```

Always calls the provider and returns validated bars in `result.data`, an eager Polars frame. It never reads or writes canonical local state. `mode` is required and accepts only the public enum values `BarFetchMode.LATEST` and `BarFetchMode.FINAL`.

`LATEST` preserves every valid row the provider returns inside the request, including a forming interval or a time-closed bar still inside Xret's finality grace. With omitted `end`, Xret requests through the exclusive boundary after the interval open at call time. Xret does not synthesize a current bar when the provider returns none.

`FINAL` filters out rows that have not passed Xret's finality grace. With omitted `end`, the grace determines the latest requested boundary. Neither mode changes the validation of identity, bounds, ordering, duplicates, OHLC relationships, or base-volume semantics, and neither mode makes a row canonical or eligible for persistence.

An endpoint with a maintained bounded-window policy traverses explicit half-open windows and can prove both present and absent bars. An endpoint without that policy uses conservative forward pagination: validated returned bars prove only their own intervals, and every unproved remainder stays `missing`. Qualification is not consulted. Ignored bounds, malformed rows, conflicting duplicates, non-progress, unsupported provider capability, or known exact incompatibility still fails explicitly.

`FetchResult` exposes `dataset_key`, `data`, `covered`, `gaps`, `source`, `warnings`, `is_complete`, and `require_complete()`. `source` records the provider, native market identity, and normalizations used by the actual call; partial evidence records `coverage.partial_observation`. Call `require_complete()` when the application requires complete remote coverage.

## `BarDataset.sync`

```python
sync(start, end=None) -> SyncResult
```

Reads local coverage, fetches only implicit `missing` intervals, validates fetched bars and observation evidence, and publishes canonical monthly Parquet files with catalog updates. `available` is persisted coverage backed by canonical bars. A successful exhaustive bounded window records each absent completed bar boundary inside that window as `unavailable`; generic or otherwise unobserved ranges remain `missing`, and failures never create negative coverage. A fully covered request is a canonical data/coverage no-op with `changed=False`, `fetched_rows=0`, and `written_partitions=0`, while still recording operational ingestion-run provenance.

The first successful synchronization binds a dataset's source lineage to the provider descriptor name. Later provider versions with the same name may extend that history; a different provider name is rejected before publication. Source lineage is an acquisition-history constraint, not part of public market identity.

`SyncResult` exposes `dataset_key`, `run_id`, `changed`, `fetched_rows`, `written_partitions`, `covered`, `gaps`, `warnings`, `source`, `is_complete`, and `require_complete()`. `source` is `None` only when no remote observation occurred.

## `BarDataset.scan`

```python
scan(start, end=None) -> polars.LazyFrame
```

Reads canonical local data only and never changes local state. It requires complete available coverage and raises `CoverageError` for any gap. With omitted `end`, the range ends at the latest local timeframe boundary.

## `BarDataset.scan_partial`

```python
scan_partial(start, end=None) -> PartialScanResult
```

Reads available canonical local data without using the provider. It is the explicit incomplete-data API. The result exposes:

- `data`: lazy frame over available rows
- `dataset_key`: resolved canonical dataset identity
- `covered`: normalized available intervals
- `gaps`: normalized `missing` or observed `unavailable` intervals
- `warnings`
- `is_complete`

An empty store returns an empty canonical lazy frame and the requested range as a `missing` gap.

## Maintenance and storage

```python
market_data.maintenance.validate()
market_data.maintenance.rebuild_catalog()
```

`validate()` compares the rebuildable SQLite operational index with canonical Parquet metadata and does not mutate state. `rebuild_catalog()` is the exclusive maintenance operation: it rebuilds SQLite only from sufficient current canonical Parquet evidence, never mutates Parquet, and fails closed when evidence is insufficient.

Catalog schema v6 identifies the closed family vocabulary and indexes open-interest contributor ownership. Xret never migrates an existing catalog in place: normal open paths reject any schema other than exactly v6 without mutation. An operator upgrades by stopping writers, backing up `state_dir` and `data_dir` together, then calling `rebuild_catalog()`. Incompatible catalogs with live WAL/SHM sidecars are not replaced. See [Migrate a local store to catalog v6](../guides/catalog-v6-migration.md) for backup, rebuild, verification, and rollback steps.

Trade bars retain their existing paths. Settled funding, reference bars, and open interest use independently versioned canonical artifacts under the reserved `_xret/settled-funding`, `_xret/reference-bars`, and `_xret/open-interest` trees. Unknown entries beneath `_xret`, malformed paths, unsupported artifact versions, and metadata/path mismatches make storage ambiguous and block operation rather than being ignored.

Rebuild restores file facts and only row-provable coverage. Trade, reference, and OI rows reconstruct contiguous `available` grids; missing slots and previous `unavailable` evidence return to `missing`. Funding rows do not prove exhaustive observation around irregular events, so funding files are indexed but funding completeness is not reconstructed. Ingestion runs, warnings, quality events, unavailable-only datasets, and other catalog-only history are not invented.

Canonical open-interest artifacts may contain ordered, non-overlapping contributor ranges from different providers. Catalog v6 reconstructs that ownership from Parquet and rejects synchronization by a provider that does not own the requested range. Other families retain one stable provider-name lineage per dataset. Xret has no automatic Parquet repair, cause taxonomy, synthetic values, in-place schema migration, or down-migration.

## 0.6.0 migration

`BarDataset.fetch()` now requires the keyword-only `mode` argument. Use `BarFetchMode.LATEST` when the application needs the provider's current observation, including a forming bar when available, and `BarFetchMode.FINAL` when it needs only rows beyond Xret's finality grace. An explicit `LATEST` range may extend into the future, but Xret accepts evidence only through the interval open when observation began; later coverage remains `missing`, and provider rows beyond that boundary are rejected. `sync()` remains final-only regardless of prior fetches and is the only bar operation that can publish canonical state.

`LiveMarketData.subscribe_bar_updates()` no longer accepts `bootstrap`. It starts only the live stream and performs no historical request, buffering, or snapshot merge. Applications that compose historical and live data own timestamp overlap, deduplication, boundary reconciliation, and reconnect backfill. Xret provides no compatibility alias because retaining `bootstrap` would preserve the removed responsibility boundary.

## 0.5.1 migration

`BarDataset.fetch()` now returns `FetchResult`; replace direct frame use with `bars.fetch(...).data` and inspect `.gaps`, `.warnings`, and `.source` when remote completeness or provenance matters. `LiveMarketData.subscribe_bar_updates()` now returns `LiveSubscription` instead of `None`. `SyncResult` adds optional `.source`.

PER-46 adds `settled_funding`, `reference_bars`, and `open_interest` as separate dataset bindings rather than columns or switches on `bars`. Each has family-specific fetch, sync, partial-scan result types and canonical schema. Applications must select nondefault providers explicitly through `MarketData(provider=...)`; provider selection never cascades or falls back. Stores created before catalog v6 require the explicit rebuild procedure above.

CCXT qualification is no longer an authorization gate or runtime state. `VerificationStatus`, `Verification`, `UnverifiedProviderWarning`, `OperationCapability.verification`, and `ProviderEvidence.verification` are removed. Applications should inspect family-specific capabilities for current availability, inspect operation results for source evidence and warnings, and call `require_complete()` when partial history is insufficient. The separate [verified-support matrix](../quality/verified-support.md) remains the dated record of scopes exercised by Xret.

## Canonical schemas

Trade bars:

```text
exchange: String, symbol: String, market: String, settle: String,
timeframe: String, timestamp: Datetime(ms, UTC),
open: Float64, high: Float64, low: Float64, close: Float64, volume: Float64
```

Settled funding:

```text
exchange: String, symbol: String, market: String, settle: String,
effective_at: Datetime(ms, UTC), funding_rate: Float64,
funding_interval_seconds: Int64?, mark_price: Float64?
```

Reference bars:

```text
exchange: String, symbol: String, market: String, settle: String,
timeframe: String, timestamp: Datetime(ms, UTC),
open: Float64, high: Float64, low: Float64, close: Float64
```

Open interest:

```text
exchange: String, symbol: String, market: String, settle: String,
timeframe: String, timestamp: Datetime(ms, UTC),
open_interest_amount: Float64, open_interest_value: Float64?
```

All timestamps are UTC millisecond instants. Identity fields are non-null for these perpetual-only families; trade-bar `settle` is null exactly for spot. Rows are strictly ordered and unique within their family identity. Nullable fields are marked `?`; Xret never substitutes a guessed interval, mark price, or OI notional.

## Derivative-family result contracts

Funding, reference-bar, and OI verbs return their exported family-specific result types rather than `FetchResult`, `SyncResult`, or `PartialScanResult`. Their common fields are:

- fetch: `dataset_key`, eager `data`, `covered`, `gaps`, `sources`, `warnings`, `is_complete`, and `require_complete()`;
- sync: the fetch fields except `data`, plus `run_id`, `changed`, `fetched_rows`, and `written_partitions`; and
- partial scan: `dataset_key`, lazy `data`, `covered`, `gaps`, `warnings`, and `is_complete`.

`FundingFetchResult`, `ReferenceBarFetchResult`, and `OpenInterestFetchResult` raise `ProviderError` from `require_complete()` when gaps remain. Their sync counterparts raise `SyncError`. Unlike the trade-bar result's optional singular `source`, derivative-family remote results expose plural `sources`: funding and reference normally contain one provider snapshot, while OI may contain multiple object/range contributors. Strict family `scan` methods return `polars.LazyFrame` and raise `CoverageError` for any gap, matching the trade-bar local-read boundary.

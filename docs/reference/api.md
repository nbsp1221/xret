# Market data API

## Package exports

`from xret.data import ...` exposes exactly:

- `MarketData`
- `MarketDataConfig`
- `BarDataset`
- `BarUpdate`
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
    provider: HistoricalBarProvider | str | None = None,
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
- `derivative`: linear/inverse and contract-size interpretation for a perpetual, otherwise `None`.
- `bar_capabilities`: per-timeframe historical and live availability plus capability notices.

`active=True` is not a guarantee that every venue operation is currently available. `timeframes` remains the concise historical catalog and is not an exhaustive-pagination or qualification claim. Inspect `bar_capabilities` to distinguish `available`, `unavailable`, and `incompatible`. Xret attempts available operations and validates the current response.

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

### `live`

```python
market_data.live(*, exchange: str) -> LiveMarketData
```

Binds a one-shot asynchronous live session for one canonical exchange and performs no I/O. Enter the returned context before subscribing:

```python
async with market_data.live(exchange="binance") as live:
    receipt = await live.subscribe_bar_updates(bars, bootstrap=True)
    print(receipt.source, receipt.warnings)
    async for update in live:
        ...
```

`bars` must be a `BarDataset` created by the same `MarketData` instance and must use the session exchange. One session may merge several bar subscriptions into its single-consumer iterator. The only current event type is immutable `BarUpdate`, containing canonical identity, timeframe, inclusive UTC bar-start timestamp, trade-derived OHLC floats, base-asset `volume`, Xret's UTC normalization receipt time, and `BarFinality` (`FORMING`, `PROVISIONAL`, or `FINAL`). Finality describes the observation relative to the bar interval and Xret's finality grace. It never claims that the value is stored as canonical data.

`subscribe_bar_updates(bars, *, bootstrap=False) -> LiveSubscription` starts live-only delivery by default and returns the resolved dataset identity, provider evidence, normalization identifiers, and initial warnings. With `bootstrap=True`, Xret buffers the activated live stream, observes the two most recent closed intervals through the same provider, coalesces timestamp overlap with the last buffered live value taking precedence, emits the bootstrap sequence in ascending timestamp order, and then continues live delivery. The operation performs remote I/O but never reads or changes canonical storage.

Same-timestamp updates are valid after bootstrap. Backward timestamps, provider failures, malformed events, and bounded queue or bootstrap-buffer overflow fail the whole session with `ProviderError`; Xret does not silently retry, reconnect, or drop old events. Ordering is nondecreasing per dataset, not globally across different datasets in one session. A non-boolean `bootstrap` value raises `InvalidRequestError` before provider I/O. See [Consume live bar updates](../guides/live-bars.md) for lifecycle and continuity guidance.

## Time ranges

Every data verb requires `start` and accepts optional `end`. Inputs may be timezone-aware `datetime` values, ISO dates, or offset-bearing ISO timestamps. Naive datetimes are rejected. Ranges are UTC-aware and half-open (`[start, end)`), and both bounds must align to the dataset timeframe.

## `BarDataset.fetch`

```python
fetch(start, end=None) -> FetchResult
```

Always calls the provider and returns validated completed bars in `result.data`, an eager Polars frame. It never reads or writes canonical local state. With omitted `end`, provider finalization grace determines the latest completed bar boundary.

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

`validate()` compares the rebuildable SQLite operational index with canonical Parquet metadata and does not mutate state. `rebuild_catalog()` is the exclusive maintenance operation: it rebuilds SQLite only from sufficient canonical Parquet evidence, never mutates Parquet, and fails closed when evidence is insufficient. Xret has no automatic repair, cause taxonomy, synthetic bars, or forensic recovery.

Canonical Parquet holds OHLCV rows and provider-neutral domain, source, and self-description metadata. SQLite uses WAL and short transactions for operational coverage, source-lineage binding, physical SHA values, file locations, and ingestion runs. Negative evidence for an unavailable-only dataset has no Parquet artifact: if its catalog is lost and rebuilt, that evidence and its lineage return to unknown rather than being invented.

Xret 0.x does not migrate incompatible canonical or catalog schemas. Normal open paths reject them without mutation. Catalog rebuild accepts only canonical Parquet written in the current schema; handling an older store is an explicit application/operator decision, not an automatic compatibility path.

## 0.5.1 migration

`BarDataset.fetch()` now returns `FetchResult`; replace direct frame use with `bars.fetch(...).data` and inspect `.gaps`, `.warnings`, and `.source` when remote completeness or provenance matters. `LiveMarketData.subscribe_bar_updates()` now returns `LiveSubscription` instead of `None`. `SyncResult` adds optional `.source`.

CCXT qualification is no longer an authorization gate or runtime state. `VerificationStatus`, `Verification`, `UnverifiedProviderWarning`, `OperationCapability.verification`, and `ProviderEvidence.verification` are removed. Applications should inspect `bar_capabilities` for current availability, inspect operation results for source and warnings, and call `require_complete()` when partial presence-only history is insufficient. The separate [verified-support matrix](../quality/verified-support.md) remains the dated record of scopes exercised by Xret.

## Canonical bar schema

Frames and canonical Parquet files use:

```text
exchange, symbol, market, settle, timeframe, timestamp,
open, high, low, close, volume
```

`timestamp` is the inclusive UTC interval start. `settle` is null exactly for spot rows. Canonical rows are unique across dataset identity plus `timestamp`.

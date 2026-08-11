# Market data API

## Package exports

`from xret.data import ...` exposes exactly:

- `MarketData`
- `MarketDataConfig`
- `BarDataset`
- `BarUpdate`
- `BarFinality`
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

Construction resolves configuration but performs no provider or storage I/O. An
explicit config bypasses configuration discovery. Omitting `provider` selects the
built-in CCXT provider. A provider object uses direct dependency injection; a
string selects an installed provider entry point. Both forms remain unresolved
until `fetch_markets`, a live context, `fetch`, or `sync` needs the provider. See
[Market-data providers](providers.md).

### `fetch_markets`

```python
market_data.fetch_markets(
    *,
    exchange: str,
    market: str,
) -> tuple[MarketDefinition, ...]
```

Fetches the selected provider's current market definitions for one canonical
venue and market-family scope. This is an eager remote metadata operation. It
never reads or changes canonical Parquet, the SQLite catalog, coverage, locks,
or source lineage, and it does not cache the returned snapshot.

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

Applications that need to name the result type in annotations import
`MarketDefinition` from the deliberate public provider-contract namespace:

```python
from xret.data.providers import MarketDefinition
```

`exchange` is a lowercase canonical slug and `market` is `spot` or
`perpetual`. They define the provider endpoint and normalization scope; they
are not search filters. The method deliberately has no query, symbol,
settlement, active-status, sorting, or pagination parameters. Callers search,
filter, sort, and cache the returned immutable tuple themselves.

Each `MarketDefinition` contains:

- `identity`: provider-independent `MarketIdentity`, including settlement for
  perpetuals;
- `active`: provider-advertised `True` or `False`, or `None` when unknown;
- `timeframes`: provider-advertised timeframe names that Xret's canonical
  grammar can express;
- `tick_size`: a positive exact `Decimal` fixed price increment, or `None`;
- `size_increment`: a positive exact `Decimal` fixed quantity increment, or
  `None`;
- `derivative`: linear/inverse and contract-size interpretation for a
  perpetual, otherwise `None`.

`active=True` is not a guarantee that every venue operation is currently
available. `timeframes` is neither an exhaustive-pagination qualification nor
a verified-support claim. Market listing, historical-bar operability, and
verified support are distinct facts.

Provider-native client IDs, derivative symbols, and raw metadata are not part
of `MarketDefinition`. Native transport failures raise chained
`ProviderError`; a selected provider without the optional market-definition
capability raises `UnsupportedMarketError`. A successful empty tuple means the
provider returned no safely representable market in the requested scope.

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

### `live`

```python
market_data.live(*, exchange: str) -> LiveMarketData
```

Binds a one-shot asynchronous live session for one canonical exchange and
performs no I/O. Enter the returned context before subscribing:

```python
async with market_data.live(exchange="binance") as live:
    await live.subscribe_bar_updates(bars, bootstrap=True)
    async for update in live:
        ...
```

`bars` must be a `BarDataset` created by the same `MarketData` instance and
must use the session exchange. One session may merge several bar subscriptions
into its single-consumer iterator. The only current event type is immutable
`BarUpdate`, containing canonical identity, timeframe, inclusive UTC bar-start
timestamp, OHLCV floats, Xret's UTC normalization receipt time, and
`BarFinality` (`FORMING`, `PROVISIONAL`, or `FINAL`). Finality describes the
observation relative to the bar interval and Xret's finality grace. It never
claims that the value is stored as canonical data.

`subscribe_bar_updates(bars, *, bootstrap=False)` starts live-only delivery by
default. With `bootstrap=True`, Xret buffers the activated live stream, observes
the two most recent closed intervals through the same provider, coalesces
timestamp overlap with the last buffered live value taking precedence, emits
the bootstrap sequence in ascending timestamp order, and then continues live
delivery. The operation performs remote I/O but never reads or changes canonical
storage.

Same-timestamp updates are valid after bootstrap. Backward timestamps, provider
failures, malformed events, and bounded queue or bootstrap-buffer overflow fail
the whole session with `ProviderError`; Xret does not silently retry, reconnect,
or drop old events. Ordering is nondecreasing per dataset, not globally across
different datasets in one session. A non-boolean `bootstrap` value raises
`InvalidRequestError` before provider I/O.
See [Consume live bar updates](../guides/live-bars.md) for lifecycle and
continuity guidance.

## Time ranges

Every data verb requires `start` and accepts optional `end`. Inputs may be timezone-aware `datetime` values, ISO dates, or offset-bearing ISO timestamps. Naive datetimes are rejected. Ranges are UTC-aware and half-open (`[start, end)`), and both bounds must align to the dataset timeframe.

## `BarDataset.fetch`

```python
fetch(start, end=None) -> polars.DataFrame
```

Always calls the provider and returns completed bars in an eager frame. It never reads or writes canonical local state. With omitted `end`, provider finalization grace determines the latest completed bar boundary.

Remote reads traverse explicit, bounded, half-open provider windows. An empty successful window is an observed empty interval, not the end of market history, so later windows are still queried. Xret returns a frame only after the requested range has been traversed exhaustively. If the CCXT endpoint family has no qualified exhaustive OHLCV pagination contract, `fetch` raises `UnsupportedMarketError` instead of returning a potentially truncated frame.

## `BarDataset.sync`

```python
sync(start, end=None) -> SyncResult
```

Reads local coverage, fetches only implicit `missing` intervals, validates fetched bars and exhaustive observation evidence, and publishes canonical monthly Parquet files with catalog updates. `available` is persisted coverage backed by canonical bars. A successful bounded provider window records each absent completed bar boundary inside that window as `unavailable`; unobserved ranges remain `missing`, and failures never create negative coverage. A fully covered request is a canonical data/coverage no-op with `changed=False`, `fetched_rows=0`, and `written_partitions=0`, while still recording operational ingestion-run provenance.

The first successful synchronization binds a dataset's source lineage to the
provider descriptor name. Later provider versions with the same name may extend
that history; a different provider name is rejected before publication. Source
lineage is an acquisition-history constraint, not part of public market identity.

`SyncResult` exposes `dataset_key`, `run_id`, `changed`, `fetched_rows`, `written_partitions`, `covered`, `gaps`, `warnings`, `is_complete`, and `require_complete()`.

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

Canonical Parquet holds OHLCV rows and provider-neutral domain, source, and
self-description metadata. SQLite uses WAL and short transactions for operational
coverage, source-lineage binding, physical SHA values, file locations, and
ingestion runs. Negative evidence for an unavailable-only dataset has no Parquet
artifact: if its catalog is lost and rebuilt, that evidence and its lineage return
to unknown rather than being invented.

Xret 0.x does not migrate incompatible canonical or catalog schemas. Normal open
paths reject them without mutation. Catalog rebuild accepts only canonical Parquet
written in the current schema; handling an older store is an explicit
application/operator decision, not an automatic compatibility path.

## Canonical bar schema

Frames and canonical Parquet files use:

```text
exchange, symbol, market, settle, timeframe, timestamp,
open, high, low, close, volume
```

`timestamp` is the inclusive UTC interval start. `settle` is null exactly for spot rows. Canonical rows are unique across dataset identity plus `timestamp`.

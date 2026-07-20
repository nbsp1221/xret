# Market data API

## Package exports

`from xret.data import ...` exposes exactly:

- `MarketData`
- `MarketDataConfig`
- `BarDataset`
- `SyncResult`
- `PartialScanResult`

## `MarketData`

```python
MarketData(config: MarketDataConfig | None = None)
```

Construction resolves configuration but performs no provider or storage I/O. An explicit config bypasses discovery.

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

## Time ranges

Every data verb requires `start` and accepts optional `end`. Inputs may be timezone-aware `datetime` values, ISO dates, or offset-bearing ISO timestamps. Naive datetimes are rejected. Ranges are UTC-aware and half-open (`[start, end)`), and both bounds must align to the dataset timeframe.

## `BarDataset.fetch`

```python
fetch(start, end=None) -> polars.DataFrame
```

Always calls the provider and returns completed bars in an eager frame. It never reads or writes canonical local state. With omitted `end`, provider finalization grace determines the latest completed bar boundary.

## `BarDataset.sync`

```python
sync(start, end=None) -> SyncResult
```

Reads local coverage, fetches only implicit `missing` intervals, validates fetched bars, and publishes canonical monthly Parquet files with catalog updates. `available` is persisted coverage backed by canonical bars. A successful provider observation records each absent completed bar boundary as `unavailable`; failures never create it. A fully covered request is a canonical data/coverage no-op with `changed=False`, `fetched_rows=0`, and `written_partitions=0`, while still recording operational ingestion-run provenance.

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

Canonical Parquet holds OHLCV rows and domain/self-description metadata. SQLite uses WAL and short transactions for operational coverage, physical SHA values, file locations, and ingestion runs.

## Canonical bar schema

Frames and canonical Parquet files use:

```text
exchange, symbol, market, settle, timeframe, timestamp,
open, high, low, close, volume
```

`timestamp` is the inclusive UTC interval start. `settle` is null exactly for spot rows. Canonical rows are unique across dataset identity plus `timestamp`.

# Synchronize and read bars

Use explicit verbs to control provider access, canonical storage, and local coverage.

## Bind one dataset

```python
from xret.data import MarketData

bars = MarketData().bars(
    exchange="binance",
    symbol="ETH/USDT",
    market="perpetual",
    settle="USDT",
    timeframe="5m",
)
```

`bars()` performs no provider or filesystem I/O. Spot datasets use `market="spot"` and omit `settle`; perpetual datasets use a settlement currency. Remote operations can infer an omitted perpetual settlement only when provider metadata has exactly one nonempty settlement value and exactly one listed perpetual market matching the base/quote and that settlement.

## Fetch without storing

```python
remote = bars.fetch(start="2025-01-01", end="2025-01-02")
```

`fetch` always uses the provider, returns an eager Polars `DataFrame` of completed bars, and never reads or changes canonical local state. It traverses bounded provider windows across the complete request; an empty intermediate window does not hide data in later windows.

## Synchronize canonical data

```python
result = bars.sync(start="2025-01-01", end="2025-02-01")
result.require_complete()

print(result.changed)
print(result.fetched_rows)
print(result.written_partitions)
```

`sync` fetches only implicit `missing` intervals and commits validated monthly canonical Parquet files. Persisted `available` means canonical bars exist. A successful exhaustive provider window records each absent completed bar boundary inside that window as `unavailable`; unobserved ranges remain `missing`, and failures never create negative coverage. Repeating a fully covered request is a canonical data/coverage no-op with `changed=False`, `fetched_rows=0`, and `written_partitions=0`, while still recording operational ingestion-run provenance.

Remote `fetch` and `sync` fail with `UnsupportedMarketError` when the selected CCXT endpoint family has no qualified exhaustive OHLCV pagination contract. Xret does not guess a page horizon or silently return a partial range.

Syncs serialize per dataset. Different datasets may overlap provider and temporary-file work.

## Require complete local coverage

```python
lazy = bars.scan(start="2025-01-01", end="2025-02-01")
frame = lazy.collect()
```

`scan` never calls the provider and does not change local state. It raises `CoverageError` for any missing or observed-unavailable interval.

## Inspect partial local coverage

```python
partial = bars.scan_partial(start="2025-01-01", end="2025-02-01")
frame = partial.data.collect()

print(partial.covered)
print(partial.gaps)
```

`scan_partial` is local-only and returns available rows with explicit coverage and gap intervals. It is the deliberate choice for incomplete local coverage.

See the [API reference](../reference/api.md) for signatures and result fields.

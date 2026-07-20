# Getting started

This walkthrough installs `xret-data`, synchronizes one perpetual market, and reads complete canonical local data.

## Install

Create a uv project and add the package:

```bash
uv init market-research
cd market-research
uv add xret-data
```

## Synchronize bars

```python
from xret.data import MarketData

market_data = MarketData()
bars = market_data.bars(
    exchange="binance",
    symbol="BTC/USDT",
    market="perpetual",
    settle="USDT",
    timeframe="1h",
)

result = bars.sync(start="2024-01-01", end="2024-02-01")
result.require_complete()
```

A spot dataset uses `market="spot"` and no `settle`. `sync` checks local coverage, fetches only missing intervals, validates bars, and commits canonical Parquet. Available coverage is persisted; a successful provider observation records each absent completed bar boundary as unavailable. A failed request does not mark data unavailable. Repeating a fully covered request is a canonical data/coverage no-op that still records operational ingestion-run provenance.

## Read complete local data

```python
frame = bars.scan(start="2024-01-01", end="2024-02-01").collect()
print(frame)
```

`scan` is strict and local-only: it raises `CoverageError` rather than returning an incomplete range. For intentional incomplete local analysis, use `scan_partial(...)`; it returns available rows and explicit gaps without using the provider.

## Continue

- [Synchronize and read bars](../guides/synchronization.md)
- [Market data API reference](../reference/api.md)
- [Data lifecycle](../explanation/data-lifecycle.md)
- [Verified support](../quality/verified-support.md)

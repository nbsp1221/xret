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
print(result.source.verification if result.source else "local no-op")
result.require_complete()
```

A spot dataset uses `market="spot"` and no `settle`. `sync` checks local coverage, fetches only missing intervals, validates bars, and commits canonical Parquet. `result.require_complete()` makes the example reject any remaining coverage gap.

The result answers two different trust questions:

- `result.source.verification` says whether Xret has independently qualified the exact remote scope. It is `None` only when the sync was already complete and made no remote call.
- `result.is_complete` says whether this requested time range has no remaining gap.

Provider-advertised but unqualified CCXT scopes may run with explicit unverified evidence. The warning does not mean Xret skipped validation, and a verified scope can still suffer a current provider or network failure. Read [Provider support and trust](../explanation/provider-support.md) before treating these states as an exchange-wide approval or rejection.

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
- [Provider support and trust](../explanation/provider-support.md)
- [Verified support](../quality/verified-support.md)

# xret-data

Trusted market data infrastructure for [Xret](https://github.com/nbsp1221/xret).

Acquire, validate, and store crypto market data with explicit I/O boundaries and fail-closed quality guarantees.

## Install

```bash
uv add xret-data
```

## Quick start

```python
from xret.data import MarketData

md = MarketData()
bars = md.bars(
    exchange="binance", symbol="BTC/USDT",
    market="perpetual", settle="USDT", timeframe="1h",
)

result = bars.sync("2024-01-01", "2024-06-01")
result.require_complete()

df = bars.scan("2024-01-01", "2024-06-01").collect()
```

## Key contracts

- `fetch` — remote-only observation, never touches local state
- `sync` — reconcile missing coverage, commit validated Parquet
- `scan` — strict local read, raises `CoverageError` on gaps
- `scan_partial` — local read with structured gap reporting

## Links

- [Documentation](https://github.com/nbsp1221/xret/tree/main/docs)
- [Verified support](https://github.com/nbsp1221/xret/blob/main/docs/quality/verified-support.md)
- [Source](https://github.com/nbsp1221/xret)

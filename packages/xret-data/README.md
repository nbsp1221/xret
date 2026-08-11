# xret-data

Trusted market data infrastructure for [Xret](https://github.com/nbsp1221/xret).

Acquire, validate, and store crypto market data with explicit I/O boundaries
and fail-closed quality guarantees. Consume typed live bar observations through
the same provider-neutral market identity.

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

Live bars use an explicit async session and never change historical storage.
Opt into a recent snapshot-to-live handoff when the consumer needs an ordered
transition from historical data:

```python
async with md.live(exchange="binance") as live:
    await live.subscribe_bar_updates(bars, bootstrap=True)
    update = await anext(live)
```

## Key contracts

- `fetch` — remote-only observation, never touches local state
- `sync` — reconcile missing coverage, commit validated Parquet
- `scan` — strict local read, raises `CoverageError` on gaps
- `scan_partial` — local read with structured gap reporting
- `live` — explicit async bar delivery and optional recent bootstrap, never writes historical state

## Links

- [Documentation](https://github.com/nbsp1221/xret/tree/main/docs)
- [Live-bar guide](https://github.com/nbsp1221/xret/blob/main/docs/guides/live-bars.md)
- [Verified support](https://github.com/nbsp1221/xret/blob/main/docs/quality/verified-support.md)
- [Source](https://github.com/nbsp1221/xret)

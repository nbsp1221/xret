# Consume live bar updates

Use a live session when an application needs current, still-forming time bars
without writing them into Xret's canonical historical store.

```python
import asyncio

from xret.data import MarketData


async def main() -> None:
    market_data = MarketData()
    btc = market_data.bars(
        exchange="binance",
        symbol="BTC/USDT",
        market="spot",
        timeframe="1m",
    )

    async with market_data.live(exchange="binance") as live:
        await live.subscribe_bar_updates(btc)

        async for update in live:
            print(update.timestamp, update.close, update.received_at)


asyncio.run(main())
```

`MarketData.live(...)` only binds the canonical exchange and performs no I/O.
Entering the context opens the provider session; subscribing resolves the
market and starts its remote stream. Leaving the context closes every native
client owned by the session.

One session may subscribe to multiple `BarDataset` values when all were created
by the same `MarketData` instance and use the session's exchange. Updates from
all subscriptions arrive through the session-wide iterator. A session is
one-shot and has one consuming task.

## Event semantics

Each `BarUpdate` contains canonical `identity` and `timeframe`, the inclusive
UTC bar-start `timestamp`, floating-point OHLCV values, and Xret's UTC
`received_at` time.

An update describes the provider's current view of a forming bar. Multiple
updates with the same timestamp are expected. It does not claim that a bar is
final, complete, durably recorded, or suitable for historical coverage.
Timestamp gaps are observable and allowed; a timestamp moving backwards for
one subscribed dataset fails the session.

## Failure and continuity

Xret does not silently reconnect, retry, coalesce events, or discard the oldest
event. A provider disconnect, malformed update, or bounded-queue overflow is a
terminal `ProviderError` for the whole session. Re-entering the same session is
invalid; the application decides whether and when to create a new one.

Live subscription never calls `sync`, writes Parquet, records catalog coverage,
or fills a disconnect gap from historical data. If continuity matters, compare
timestamps and reconcile explicitly through a new historical operation.

The initial surface supports time-bar updates only. Trades, quotes, order books,
raw provider payloads, exchange sequence numbers, finality flags, recording,
replay, fan-out servers, and order execution are outside this contract.

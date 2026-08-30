# Consume live bar updates

Use a live session when an application needs current time-bar observations without writing them into Xret's canonical historical store.

```python
import asyncio

from xret.data import BarFinality, MarketData


async def main() -> None:
    market_data = MarketData()
    btc = market_data.bars(
        exchange="binance",
        symbol="BTC/USDT",
        market="spot",
        timeframe="1m",
    )

    async with market_data.live(exchange="binance") as live:
        receipt = await live.subscribe_bar_updates(btc)
        print(receipt.source, receipt.warnings)

        async for update in live:
            print(update.timestamp, update.close, update.finality)
            if update.finality is BarFinality.FORMING:
                print("current bar may still change")


asyncio.run(main())
```

`MarketData.live(...)` only binds the canonical exchange and performs no I/O. Entering the context opens the provider session; subscribing resolves the market, checks provider capability, starts its remote stream, and returns a `LiveSubscription` receipt with source, normalization, and warning evidence. Leaving the context closes every native client owned by the session.

CCXT Pro scopes are capability-based. A provider-advertised `watchOHLCV` scope can run even when it is absent from Xret's published qualification matrix. Xret does not load that matrix into the session or warn merely because a scope lacks prior QA evidence. Missing capability or timeframe, a known exact incompatibility, or unsafe runtime data still fails explicitly.

The published live list is therefore evidence, not a three-exchange allowlist. Binance, Bybit, and OKX are the currently qualified venues, while another provider-advertised live scope may also be attempted. The subscription receipt describes the operation that actually ran; the [verified-support matrix](../quality/verified-support.md#currently-verified-live-bars) describes the narrower set with prior Xret QA evidence.

One session may subscribe to multiple `BarDataset` values when all were created by the same `MarketData` instance and use the session's exchange. Updates from all subscriptions arrive through the session-wide iterator. A session is one-shot and has one consuming task.

Subscription is live-only. It does not request historical bars, wait for the first market-data event, buffer a snapshot handoff, or write canonical storage. Returning from `subscribe_bar_updates()` means the provider session's subscription method completed; it is not a portable provider-level acknowledgement and does not promise that an event will arrive within a particular latency. A healthy event-driven stream can remain quiet until the next trade.

## Event semantics

Each `BarUpdate` contains canonical `identity` and `timeframe`, the inclusive UTC bar-start `timestamp`, trade-derived floating-point OHLC values, base-asset `volume`, Xret's UTC `received_at` time, and a `BarFinality` value. See the [canonical time-bar contract](../reference/time-bars.md) for exact interval and unit semantics.

- `FORMING`: Xret received the observation before the bar interval ended.
- `PROVISIONAL`: the interval ended, but Xret's finality grace has not elapsed.
- `FINAL`: the observation passed the grace at receipt time.

Finality is an observation-time classification, not persistence state or a provider sequence guarantee. Even a `FINAL` update is not canonical data until an explicit later `sync()` reacquires, validates, and commits that timestamp. Multiple updates with the same timestamp are expected. Timestamp gaps are observable and allowed; a timestamp moving backwards for one subscribed dataset fails the session. Ordering is nondecreasing per dataset; a session with several datasets does not promise global event-time ordering between them.

## Compose history and live data

Use `BarFetchMode.LATEST` when an application wants the provider's current historical view before or around a live subscription. The result may include the forming bar, and a later live update with the same timestamp is a replacement candidate rather than a duplicate canonical row.

`fetch()` and live subscription are independent primitives. Xret does not guarantee an atomic handoff between them. The application decides whether to fetch before subscribing, subscribe while buffering its own events and then fetch, or refetch a recent range after connecting. It also owns timestamp overlap, deduplication, stale forming-bar replacement, and any stronger continuity policy required by its UI or trading runtime.

## Failure and continuity

Xret does not silently reconnect, retry, coalesce events, or discard the oldest event. A provider disconnect, malformed update, or bounded-queue overflow is a terminal `ProviderError` for the whole session. Re-entering the same session is invalid; the application decides whether and when to create a new one, refetch a recent range, and reconcile any disconnect gap.

Live subscription never calls `fetch` or `sync`, writes Parquet, or records catalog coverage.

The current surface supports time-bar updates only. Trades, quotes, order books, raw provider payloads, exchange sequence numbers, long-range replay, recording, fan-out servers, and order execution are outside this contract.

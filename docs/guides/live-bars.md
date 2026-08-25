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
        receipt = await live.subscribe_bar_updates(btc, bootstrap=True)
        print(receipt.source.verification, receipt.warnings)

        async for update in live:
            print(update.timestamp, update.close, update.finality)
            if update.finality is BarFinality.FORMING:
                print("current bar may still change")


asyncio.run(main())
```

`MarketData.live(...)` only binds the canonical exchange and performs no I/O. Entering the context opens the provider session; subscribing resolves the market, checks provider capability, starts its remote stream, and returns a `LiveSubscription` receipt with source, verification, normalization, and warning evidence. Leaving the context closes every native client owned by the session.

CCXT Pro scopes are capability-based. A provider-advertised `watchOHLCV` scope can run even when it is absent from Xret's verified matrix; the receipt reports it as `unverified` and Xret emits one `UnverifiedProviderWarning` for the subscription. Verification never bypasses validation. Missing capability or timeframe, a known exact incompatibility, or unsafe runtime data still fails explicitly.

This means the verified live list is not a three-exchange allowlist. Binance, Bybit, and OKX are the currently qualified venues, while another provider-advertised live scope may still be attempted as unverified. The subscription receipt describes the exact operation that ran; the [verified-support matrix](../quality/verified-support.md#currently-verified-live-bars) describes the narrower set with independent Xret evidence.

One session may subscribe to multiple `BarDataset` values when all were created by the same `MarketData` instance and use the session's exchange. Updates from all subscriptions arrive through the session-wide iterator. A session is one-shot and has one consuming task.

`bootstrap=True` is optional. Without it, the subscription requires only live capability and starts with whatever the provider sends after the stream is activated. With it, Xret additionally requires historical capability for the same scope, first buffers live updates, observes the two most recent closed bar intervals through the same provider, validates and merges timestamp overlaps, and publishes the result in ascending timestamp order before continuing live delivery. This closes the transition race between a completed historical request and a newly opened live stream; it is not a TradingView-specific protocol.

Bootstrap waits for the first live update as evidence that the stream is active. On an illiquid stream this can take time, so apply an application latency budget with `asyncio.timeout()` when needed. Xret does not synthesize a bar for an interval where the provider proves no bar exists.

## Event semantics

Each `BarUpdate` contains canonical `identity` and `timeframe`, the inclusive UTC bar-start `timestamp`, trade-derived floating-point OHLC values, base-asset `volume`, Xret's UTC `received_at` time, and a `BarFinality` value. See the [canonical time-bar contract](../reference/time-bars.md) for exact interval and unit semantics.

- `FORMING`: Xret received the observation before the bar interval ended.
- `PROVISIONAL`: the interval ended, but Xret's finality grace has not elapsed.
- `FINAL`: the observation passed the grace at receipt time.

Finality is an observation-time classification, not persistence state or a provider sequence guarantee. Even a `FINAL` update is not canonical data until an explicit later `sync()` reacquires, validates, and commits that timestamp. Multiple updates with the same timestamp are expected. Timestamp gaps are observable and allowed; a timestamp moving backwards for one subscribed dataset fails the session.

During bootstrap only, Xret coalesces overlap by timestamp. The last buffered live full-state update wins over a recent snapshot row for the same timestamp. After bootstrap, same-timestamp revisions are delivered normally. Ordering is per dataset; a session with several datasets does not promise global event-time ordering between them.

## Failure and continuity

Xret does not silently reconnect, retry, coalesce events, or discard the oldest event. A provider disconnect, malformed update, or bounded-queue overflow is a terminal `ProviderError` for the whole session. Re-entering the same session is invalid; the application decides whether and when to create a new one.

Live subscription and bootstrap never call `sync`, write Parquet, or record catalog coverage. Bootstrap handles only the initial recent snapshot-to-live handoff. It does not fill a later disconnect gap. After a disconnect, create a new session and reconcile according to the application's recovery policy.

The current surface supports time-bar updates only. Trades, quotes, order books, raw provider payloads, exchange sequence numbers, long-range replay, recording, fan-out servers, and order execution are outside this contract.

# Provider support and trust

Xret does not maintain an exchange allowlist. It attempts a CCXT operation when the provider advertises the required capability and Xret can express the requested market and timeframe without a known semantic loss. Every returned observation then passes Xret's runtime validation before it is returned or stored.

This design separates access from confidence: an operation can be available without having current Xret qualification evidence.

## The three questions Xret answers

“Does Xret support this exchange?” is too broad to have one reliable yes-or-no answer. Support is evaluated for an exact venue, market family, settlement, timeframe, and operation.

| Question | Xret evidence | What it means |
|---|---|---|
| Can Xret attempt this operation? | `OperationCapability.availability` | The provider advertises the operation and Xret has no known lossless-contract conflict. |
| Did this call produce safe data? | Return value or raised exception | Every response is checked at runtime. Unsafe data fails instead of being returned or committed. |
| Has Xret independently exercised this exact scope? | `Verification.status` | `verified` records current qualification evidence; `unverified` means that evidence is absent, not that execution is denied. |

Availability and verification are deliberately independent. A historical qualification never implies live qualification, a spot qualification never implies perpetual qualification, and evidence for one settlement or timeframe never silently approves another.

## What happens at runtime

| Capability state | Xret behavior |
|---|---|
| `available` + `verified` | Attempt the operation, validate the response, and return verified provider evidence. |
| `available` + `unverified` | Attempt the operation, emit `UnverifiedProviderWarning`, validate the response, and return structured unverified evidence. |
| `unavailable` | Raise `UnsupportedMarketError`; the provider does not advertise the required capability or timeframe. |
| `incompatible` | Raise `UnsupportedMarketError`; Xret has evidence that the exact scope cannot satisfy the canonical contract losslessly. |
| Unsafe response during an attempted operation | Fail explicitly with a domain error before unsafe data is returned or committed. |

Verification is not a bypass around validation and is not a promise of future provider uptime. Providers can change, restrict access by region, rate-limit requests, delist instruments, or return a transient failure after qualification. Conversely, `unverified` is not a synonym for broken: it says only that the exact scope is outside Xret's current independent evidence set.

## Historical coverage is a separate result

A successful historical call can still be incomplete. Some endpoints support exact bounded traversal, while others expose only recent or presence-only history. `FetchResult` and `SyncResult` therefore report `covered`, `gaps`, and `is_complete` separately from provider verification.

```python
from xret.data import MarketData, VerificationStatus

bars = MarketData().bars(
    exchange="kraken",
    symbol="BTC/USD",
    market="spot",
    timeframe="1m",
)

result = bars.fetch("2026-08-24T00:00:00Z", "2026-08-24T00:05:00Z")

print(result.source.verification.status)
print(result.is_complete, result.gaps)

if result.source.verification.status is VerificationStatus.UNVERIFIED:
    print("This scope ran with runtime validation but without Xret qualification.")

result.require_complete()
```

`require_complete()` answers whether this requested range was fully proved. It does not change the verification status. Likewise, `verified` describes qualification evidence and does not make a partial result complete.

## Discover before operating

`fetch_markets()` exposes historical and live capability facts for every representable timeframe in the current provider snapshot:

```python
from xret.data import MarketData

market_data = MarketData()
definitions = market_data.fetch_markets(exchange="hyperliquid", market="perpetual")

for definition in definitions:
    if definition.identity.symbol != "BTC/USDC":
        continue
    for bars in definition.bar_capabilities:
        print(
            bars.timeframe,
            bars.historical.availability,
            bars.historical.verification,
            bars.live.availability,
            bars.live.verification,
        )
```

Discovery is a current provider metadata snapshot, not an uptime probe or a guarantee that credentials, geography, rate limits, and the venue will permit the later request. The operation result remains the authoritative evidence for what actually happened.

## What Xret guarantees

For every attempted remote time-bar operation, Xret keeps these responsibilities:

- provider-independent spot and perpetual market identity;
- UTC-aligned canonical interval semantics;
- trade-derived OHLC and base-asset volume;
- schema, timestamp, ordering, duplicate, range, and OHLC invariant validation;
- explicit incomplete coverage instead of invented bars or silent gap filling;
- explicit source, normalization, verification, and warning evidence; and
- fail-closed canonical publication.

Xret does not guarantee exchange uptime, complete history where the provider cannot prove it, uninterrupted WebSocket delivery, automatic reconnect, or semantic correctness that cannot be established from the received evidence. The [canonical time-bar contract](../reference/time-bars.md) defines what accepted data means; the [verified-support matrix](../quality/verified-support.md) lists the exact scopes Xret has independently exercised.

## A practical decision rule

- Use a `verified` scope when you want Xret's strongest existing provider evidence.
- Use an `unverified` scope when broader access is worth independently evaluating the warning and result evidence.
- Require `is_complete` or call `require_complete()` whenever a complete historical range is mandatory.
- Treat `unavailable` as a missing advertised capability and `incompatible` as a known semantic boundary.
- Handle provider and live-session failures as normal operational failures; qualification cannot eliminate them.

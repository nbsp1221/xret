# Provider support and trust

Xret does not maintain an exchange allowlist. It attempts a provider operation when the provider advertises the required capability and Xret can express the requested market and timeframe without a known semantic loss. Every returned observation then passes Xret's runtime validation before it is returned or stored.

Prior qualification is useful evidence, but it is not product state. Xret publishes combinations exercised against real providers in [Verified support](../quality/verified-support.md); the runtime does not load that matrix, attach a verified or unverified label, warn because an entry is absent, or use qualification to authorize execution.

## The four questions

“Does Xret support this exchange?” is too broad to have one reliable yes-or-no answer. Evaluate an exact venue, market family, settlement, timeframe, and operation through four separate questions.

| Question | Evidence | What it means |
|---|---|---|
| Can Xret attempt this operation? | `OperationCapability.availability` | The provider advertises the operation and Xret has no known lossless-contract conflict. |
| Did this call produce safe data? | Return value or raised exception | Every actual response is validated. Unsafe data fails instead of being returned or committed. |
| Is this requested historical range complete? | `covered`, `gaps`, `is_complete`, and `require_complete()` | Xret distinguishes proved observations from unproved or observed-unavailable intervals. |
| Has Xret exercised this scope before? | The public verified-support matrix | This is dated QA evidence for a precise scope, not runtime authorization or a future guarantee. |

These answers are deliberately independent. A provider can advertise an operation that fails today. A valid response can cover only part of the requested range. A previously qualified scope can suffer a provider regression, while a scope absent from the matrix can work correctly and pass the same runtime checks.

## What happens at runtime

| Current fact | Xret behavior |
|---|---|
| `available` | Attempt the operation and validate its response. |
| `unavailable` | Raise `UnsupportedMarketError`; the provider does not advertise the required capability or timeframe. |
| `incompatible` | Raise `UnsupportedMarketError`; Xret has evidence that the exact scope cannot satisfy the canonical contract losslessly. |
| Unsafe response | Fail explicitly with a domain error before unsafe data is returned or committed. |
| Incomplete historical evidence | Return explicit gaps; `require_complete()` raises when the caller requires the full range. |

Warnings report facts observed during the current operation, such as partial coverage or a partial live bootstrap. Xret does not warn merely because it has not previously qualified an otherwise available scope.

## Historical coverage is separate

A successful historical call can still be incomplete. Some endpoints support exact bounded traversal, while others expose only recent or presence-only history. `FetchResult` and `SyncResult` therefore report `covered`, `gaps`, and `is_complete`.

```python
from xret.data import MarketData

bars = MarketData().bars(
    exchange="kraken",
    symbol="BTC/USD",
    market="spot",
    timeframe="1m",
)

result = bars.fetch("2026-08-24T00:00:00Z", "2026-08-24T00:05:00Z")

print(result.source)
print(result.is_complete, result.gaps)
result.require_complete()
```

`result.source` records the provider, native market identity, and applied normalizations for the call that ran. `require_complete()` answers only whether this requested range was fully proved.

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
            bars.historical.notices,
            bars.live.availability,
            bars.live.notices,
        )
```

Discovery is a current provider metadata snapshot, not an uptime probe or a guarantee that credentials, geography, rate limits, and the venue will permit the later request. The operation result remains authoritative for what actually happened.

## What Xret guarantees

For every attempted remote time-bar operation, Xret keeps these responsibilities:

- provider-independent spot and perpetual market identity;
- UTC-aligned canonical interval semantics;
- trade-derived OHLC and base-asset volume;
- schema, timestamp, ordering, duplicate, range, and OHLC invariant validation;
- explicit incomplete coverage instead of invented bars or silent gap filling;
- explicit source, normalization, coverage, and current warning evidence; and
- fail-closed canonical publication.

Xret does not guarantee exchange uptime, complete history where the provider cannot prove it, uninterrupted WebSocket delivery, automatic reconnect, or semantic correctness that cannot be established from received evidence and maintained compatibility rules. The [canonical time-bar contract](../reference/time-bars.md) defines what accepted data means; the [verified-support matrix](../quality/verified-support.md) records the exact scopes Xret has independently exercised.

## A practical decision rule

- Use capability discovery to learn what Xret can attempt now.
- Require `is_complete` or call `require_complete()` whenever a complete historical range is mandatory.
- Treat `unavailable` as a missing advertised capability and `incompatible` as a known semantic boundary.
- Inspect source, normalizations, warnings, and gaps from the operation that actually ran.
- Use the verified-support matrix as additional dated confidence evidence, never as a substitute for current runtime results.
- Handle provider and live-session failures as normal operational failures; prior qualification cannot eliminate them.

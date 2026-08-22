# Verified support

`verified` means that Xret has been exercised as an external user would use it against the real provider, beyond repository-controlled automated tests.

## Verification criterion

A provider combination is verified only after human-style dogfooding succeeds in a fresh project outside the repository. The evaluator installs the built `xret-data` distribution through uv, uses the public API against the real network, inspects results, and adapts the investigation when behavior warrants additional checks.

The evaluation must be broad enough to exercise the material risks of the provider combination without pretending to test every symbol. It covers representative symbols and the publicly claimed timeframes across realistic short and multi-year ranges, and separately proves the [canonical trade time-bar semantics](../reference/time-bars.md), including as applicable:

- initial acquisition and canonical storage;
- exact strict reads and partial reads;
- incremental extension;
- an identical no-op synchronization;
- partition and calendar boundaries;
- catalog validation and file-derived catalog/strict-read equivalence after rebuild;
- concurrent synchronization of the same dataset; and
- behavior at listing, availability, or other relevant provider boundaries.

The evaluator must also prove that OHLC comes from the eligible executed-trade universe, `volume` is exact base-asset quantity, and every native interval's origin, timezone, label, and finality match Xret. The evaluator must find no unexplained gaps, duplicate timestamps, out-of-range rows, incomplete final bars, schema violations, invalid OHLC relationships, rebuild mismatches, or unresolved warnings.

Automated unit, integration, and heavy E2E tests are engineering prerequisites. Their definitions and results are already represented by test configuration, local command output, and CI logs; they are not repeated as verified promotion criteria.

## Verification granularity

Verification applies to a specific combination of:

```text
provider adapter
+ venue endpoint family
+ market family
+ bar type
+ exact timeframe
```

Representative symbols exercise shared adapter behavior. Verification does not mean that only those exact symbols are supported, and evidence from one endpoint or contract family is never generalized to an entire exchange.

This matrix is a public trust statement, not a runtime allowlist. Provider adapters do not reject an otherwise valid venue merely because it is absent here. Conversely, successfully fetching a provider's market definitions, an `active=True` value, or an advertised timeframe does not prove that Xret can exhaustively acquire historical bars for that combination. Market-definition availability, historical-bar operability, and verified support remain separate facts.

## Qualification status at a glance

The complete canonical historical trust set is listed below. Every entry passed the maintained live-network gate with all mandatory edge cases, 135 additional deterministic observations at the declared 99.9% confidence target, contract-correct strict and partial coverage behavior, and an independent public-trade/candle semantic witness for every available market family.

The final 2026-08-23 promotion run installed the built `xret-data` 0.4.0 wheel in a fresh uv project outside the repository and ran against CCXT 4.5.75. The eleven full-venue scopes completed together in 352.49 seconds with 9,306 OHLCV calls and 767,761 returned rows; the independently scoped BingX perpetual and Bitrue spot runs also passed in 195.50 and 103.14 seconds respectively.

| Status | Venues |
|---|---|
| Complete canonical historical qualification | ApeX, Binance, BingX perpetual, Bitget, Bitrue spot, Bitvavo, Bybit, Coinbase, Deribit, HashKey, MEXC, OKX, Toobit |
| Earlier evidence only; current strengthened gate not passed | Gate, WOO |

Live-bar verification is separate and appears under [Currently verified live bars](#currently-verified-live-bars). A historical qualification never implies live support.

## Complete canonical historical qualification

| Venue | Market families | Qualified timeframes |
|---|---|---|
| ApeX | USDT-settled linear perpetual | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` |
| Binance | Spot; USDT-settled linear perpetual | Spot: `1s`, `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M`; perpetual: the same except `1s` |
| BingX | USDT-settled linear perpetual only | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `3d`, `1w` |
| Bitget | Spot; USDT-settled linear perpetual | Spot: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `4h`, `6h`, `12h`, `1d`, `3d`, `1w`, `1M`; perpetual additionally includes `2h` |
| Bitrue | Spot only | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h` |
| Bitvavo | Spot | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d` |
| Bybit | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` |
| Coinbase | Spot; USDC-settled linear perpetual | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `6h`, `1d` |
| Deribit | Spot; USDC-settled linear perpetual | `1m`, `3m`, `5m`, `10m`, `15m`, `30m`, `1h`, `2h` |
| HashKey | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M` |
| MEXC | Spot; USDT-settled linear perpetual | Spot: `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`, `1w`; perpetual additionally includes `8h`, `1M` |
| OKX | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` |
| Toobit | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M` |

The qualification excludes only combinations that cannot satisfy the canonical contract losslessly. Binance `3d`, Binance USD-M `1s`/`3d`, BingX perpetual `1M`, Bitget spot `2h`, Deribit `3h`/`6h`/`12h`/`1d`, and MEXC spot `8h`/`1M` are therefore not advertised by Xret. BingX spot remains unqualified because its long-interval anchors and candle volume do not match the canonical contract. Exact base volume is reconstructed from verified contract metadata for linear ApeX, HashKey, MEXC, and Toobit perpetuals; inverse contracts whose aggregate candle volume cannot be converted exactly are excluded.

Native no-trade, pre-listing, and retention gaps remain explicit unavailable coverage. No venue-specific storage exception, synthetic bar, or gap waiver is present in Xret.

## Earlier evidence not promoted by the current gate

Gate and WOO retain earlier narrower `1h` evidence but are not in the complete trust set. Bitrue perpetuals were not promoted by the current gate; that missing futures scope remains explicit rather than being inferred from the now-qualified spot endpoint.

## Currently verified live bars

The live-bar matrix below was requalified on 2026-08-11 from the built `xret-data` 0.3.0 wheel in a fresh external uv project with CCXT 4.5.71. Each combination opened through the public API, completed an initial bootstrap with two recent closed bars followed by the current forming bar in ascending, duplicate-free timestamp order, and closed cleanly without creating canonical state or catalog paths.

| Provider | Venue | Market family | Bar type | Timeframe | Representative symbol |
|---|---|---|---|---|---|
| CCXT Pro | Binance | Spot | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | Binance USD-M | USDT-settled linear perpetual | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | Bybit | Spot | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | Bybit | USDT-settled linear perpetual | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | OKX | Spot | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |
| CCXT Pro | OKX | USDT-settled linear perpetual | Initial snapshot-to-live time-bar handoff | `1m` | `BTC/USDT` |

The Binance USD-M qualification additionally exercised the handoff immediately after a one-minute boundary, while the just-closed bar remained provisional under the finality grace period. It produced consecutive `FINAL`, `PROVISIONAL`, and `FORMING` observations without touching canonical storage.

This is a connectivity, structural normalization, initial-handoff, multiplexing, and lifecycle claim. It does not independently prove the executed-trade event universe or base-asset volume unit, and it does not prove uninterrupted continuity, exhaustive delivery, canonical persistence of live observations, reconnect behavior, or long-running stability. Xret exposes disconnects and overflow as terminal failures rather than extending this matrix into those claims.

## Re-verification

Human-style re-verification is required when a change can materially alter real provider behavior, including:

- provider adapter or endpoint-family changes;
- market or symbol resolution changes;
- pagination changes;
- timestamp, timeframe, or finalization changes;
- canonical quality or normalization changes;
- commit, coverage, catalog, locking, or recovery changes;
- a provider dependency major upgrade; or
- a venue migration to a materially different endpoint.

Documentation-only changes and internal refactors that cannot affect these boundaries do not require renewed dogfooding.

## Evidence retention

The stable criterion and verified combinations are tracked here. Ad hoc scripts, temporary projects, raw network output, intermediate failures, local paths, large data stores, and detailed QA reports are transient internal evidence and are not committed to Git.

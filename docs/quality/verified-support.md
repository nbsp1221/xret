# Verified support

`verified` means that Xret has been exercised as an external user would use it against the real provider, beyond repository-controlled automated tests.

## Verification criterion

A provider combination is verified only after human-style dogfooding succeeds in a fresh project outside the repository. The evaluator installs the built `xret-data` distribution through uv, uses the public API against the real network, inspects results, and adapts the investigation when behavior warrants additional checks.

The evaluation must be broad enough to exercise the material risks of the provider combination without pretending to test every symbol. It covers representative symbols and the publicly claimed timeframes across realistic short and multi-year ranges, including as applicable:

- initial acquisition and canonical storage;
- exact strict reads and partial reads;
- incremental extension;
- an identical no-op synchronization;
- partition and calendar boundaries;
- catalog validation and file-derived catalog/strict-read equivalence after rebuild;
- concurrent synchronization of the same dataset; and
- behavior at listing, availability, or other relevant provider boundaries.

The evaluator must find no unexplained gaps, duplicate timestamps, out-of-range rows, incomplete final bars, schema violations, invalid OHLC relationships, rebuild mismatches, or unresolved warnings.

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

## Currently verified
The currently listed matrix combines the earlier long-range qualifications with the 2026-08-23 qualification gate for the newly added CCXT endpoint families. The new gate used the built `xret-data` 0.4.0 distribution in a fresh external uv environment with CCXT 4.5.75, and exercised representative spot/perpetual markets over recent, seven-day, and one-year ranges, including fetch, sync, strict scan, idempotent sync, partial scan, and catalog validation/rebuild. Each qualification records the provider dependency version it exercised, because a provider release can change endpoint behavior without any Xret change.

The autonomous requalification batch on 2026-08-23 independently checked every remaining gate `coverage_review` interval for OKX and Bybit against each venue's official market metadata and candle endpoint. The intervals were confirmed as native listing or contract-launch boundaries. They remain explicit unavailable coverage in Xret; no exchange-specific fill or scan exception was added. Accordingly, the OKX and Bybit claims below are coverage-qualified for the exercised endpoint families and representative timeframe/symbol sample, with the documented historical boundary caveat rather than an unexplained failure.

### Binance

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | Binance USD-M klines | USDT-settled linear perpetual | Time bars | `1h` | `BTC/USDT` |

Binance USD-M perpetual `1h` qualification covers complete four-year synchronization across monthly partitions, incremental extension, exact and concurrent canonical-data no-op synchronization, strict/partial read equivalence, bar invariants, validation, and file-derived catalog/strict-read equivalence after rebuild. Binance spot combinations remain unlisted pending re-verification after the storage-contract changes.

### Bybit

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | Bybit spot kline | Spot | Time bars | `1h` | `BTC/USDT` |
| CCXT | Bybit derivatives kline | USDT-settled linear perpetual | Time bars | `1h` | `BTC/USDT` |

Bybit spot and perpetual `1h` qualification covers complete four-year synchronization across monthly partitions, incremental extension, exact and concurrent canonical-data no-op synchronization, strict/partial read equivalence, bar invariants, validation, and file-derived catalog/strict-read equivalence after rebuild. Bybit `1m` remains unlisted pending equivalent re-verification.

### OKX

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | OKX market candles | Spot | Time bars | `1h` | `BTC/USDT` |

OKX spot `1h` qualification covers complete four-year synchronization across monthly partitions, incremental extension, exact and concurrent canonical-data no-op synchronization, strict/partial read equivalence, bar invariants, validation, and file-derived catalog/strict-read equivalence after rebuild. OKX perpetual combinations remain unlisted pending equivalent re-verification.

### Gate

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | Gate spot OHLCV | Spot | Time bars | `1h` | `BTC/USDT` |
| CCXT | Gate perpetual OHLCV | USDT-settled linear perpetual | Time bars | `1h` | `BTC/USDT` |

Gate spot and perpetual `1h` passed the 2026-08-23 qualification gate over recent, seven-day, and one-year ranges, including canonical sync, strict reads, idempotent synchronization, partial reads, and catalog validation/rebuild.

### HashKey

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | HashKey spot OHLCV | Spot | Time bars | `1h` | `BTC/USDT` |
| CCXT | HashKey perpetual OHLCV | USDT-settled linear perpetual | Time bars | `1h` | `BTC/USDT` |

HashKey spot and perpetual `1h` passed the 2026-08-23 qualification gate over recent, seven-day, and one-year ranges, including canonical sync, strict reads, idempotent synchronization, partial reads, and catalog validation/rebuild.

### WOO

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | WOO spot OHLCV | Spot | Time bars | `1h` | `BTC/USDT` |
| CCXT | WOO perpetual OHLCV | USDT-settled linear perpetual | Time bars | `1h` | `BTC/USDT` |

WOO spot and perpetual `1h` passed the 2026-08-23 qualification gate over recent, seven-day, and one-year ranges, including canonical sync, strict reads, idempotent synchronization, partial reads, and catalog validation/rebuild.

### Bitrue

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | Bitrue spot OHLCV | Spot | Time bars | `1h` | `BTC/USDT` |

Bitrue spot `1h` passed the 2026-08-23 qualification gate over recent, seven-day, and one-year ranges, including canonical sync, strict reads, idempotent synchronization, partial reads, and catalog validation/rebuild. No perpetual market was selected by the gate, so this claim does not cover Bitrue perpetuals.

A rebuild restores Parquet-provable datasets, files, and available coverage; it intentionally does not restore unavailable observations, ingestion runs, warnings, or quality events.

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

This is a connectivity, normalization, initial-handoff, multiplexing, and lifecycle claim. It does not prove uninterrupted continuity, exhaustive delivery, canonical persistence of live observations, reconnect behavior, or long-running stability. Xret exposes disconnects and overflow as terminal failures rather than extending this matrix into those claims.

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

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

## Qualification status at a glance

Historical support is divided into two evidence tiers. Only the first row is the current strengthened trust set.

| Evidence tier | Venues | Meaning |
|---|---|---|
| Strengthened historical qualification | Coinbase, OKX, HashKey | Passed every mandatory edge case and 135 additional statistical observations at the declared 99.9% confidence target, with every coverage review independently explained using official venue evidence |
| Legacy historical verification | Binance, Bybit, Gate, WOO, Bitrue | Passed an earlier narrower qualification, but has not passed the current strengthened gate and must not be presented as equivalent evidence |

Live-bar verification is separate and appears under [Currently verified live bars](#currently-verified-live-bars). A historical qualification never implies live support.

## Strengthened historical qualification — 99.9% gate

The current strengthened trust set contains exactly three venues: **Coinbase, OKX, and HashKey**.

The 2026-08-23 campaign used the built `xret-data` 0.4.0 distribution in a fresh external uv environment with CCXT 4.5.75. Each venue ran every CCXT-advertised timeframe expressible by Xret, all mandatory boundary and lifecycle cases, and 135 additional deterministic statistical observations: the zero-failure sample target for a one-sided 99.9% bound below a 5% invariant-violation rate under the declared sampling model. Mandatory cases included one- and two-bar ranges, page-limit boundaries, calendar boundaries, long history, incremental and no-op synchronization, concurrent synchronization, strict and partial scans, invalid ranges, and catalog validation/rebuild.

Every remaining `coverage_review` was compared with official market metadata or candle endpoints. Coinbase's interior one-minute gaps matched its native sparse candle response; the other reviewed intervals matched native listing, venue-launch, or contract-launch boundaries. These intervals remain explicit unavailable coverage in Xret, and no exchange-specific fill or scan exception was added.

### Strengthened trust set summary

| Venue | Verified market families | Verified timeframes | Status |
|---|---|---|---|
| Coinbase | Spot; USDC-settled linear perpetual | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `6h`, `1d` | Strengthened gate passed |
| OKX | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` | Strengthened gate passed |
| HashKey | Spot; USDT-settled linear perpetual | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M` | Strengthened gate passed |

### Coinbase

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | Coinbase candles | Spot | Time bars | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `6h`, `1d` | `BTC/USDT`, `BTC/USDC`, `ETH/USDT`, `ETH/USDC`, `SOL/USDT`, `SOL/USDC` |
| CCXT | Coinbase candles | USDC-settled linear perpetual | Time bars | `1m`, `5m`, `15m`, `30m`, `1h`, `2h`, `6h`, `1d` | `BTC/USDC`, `ETH/USDC`, `SOL/USDC`, `DOGE/USDC`, `XRP/USDC`, `1000PEPE/USDC` |

Coinbase passed with no unexplained invariant violation. Direct comparison with the official candle endpoint confirmed that the observed spot `BTC/USDT` one-minute interior gaps were native sparse no-trade intervals rather than CCXT or Xret data loss. Product-history prefixes remain explicit unavailable coverage; Xret does not fill or exempt them.

### OKX

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | OKX market candles | Spot | Time bars | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` | `BTC/USDT`, `BTC/USDC`, `ETH/USDT`, `ETH/USDC`, `SOL/USDT`, `SOL/USDC` |
| CCXT | OKX market candles | USDT-settled linear perpetual | Time bars | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`, `1w`, `1M` | `BTC/USDT`, `ETH/USDT`, `SOL/USDT` |

OKX passed with no unexplained invariant violation. Direct official market metadata confirmed that the observed USDC spot and perpetual history prefixes match product listing or contract launch times. Those prefixes remain explicit unavailable coverage; Xret does not fill or exempt them. Inverse perpetual settlements were sampled but remain outside the strengthened trust set pending an equally complete settlement-specific promotion review.

### HashKey

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | HashKey spot OHLCV | Spot | Time bars | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M` | `BTC/USDT`, `ETH/USDT`, `SOL/USDT`, `AVAX/USDT`, `0G/USDT`, `AGI/USDT` |
| CCXT | HashKey perpetual OHLCV | USDT-settled linear perpetual | Time bars | `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `1w`, `1M` | `BTC/USDT`, `ETH/USDT` |

HashKey passed with no unexplained invariant violation. Direct official market and candle endpoints confirmed that the observed history prefixes match venue or product launch dates. Those prefixes remain explicit unavailable coverage; Xret does not fill or exempt them. USD-settled perpetuals were sampled but remain outside the strengthened trust set pending an equally complete settlement-specific promotion review.

## Legacy historical verification — strengthened requalification required

The combinations below remain documented because they passed their original external qualification. They are not members of the 99.9% strengthened trust set. They must pass the current gate before being promoted to that tier.

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

Bybit spot and perpetual `1h` qualification covers complete four-year synchronization across monthly partitions, incremental extension, exact and concurrent canonical-data no-op synchronization, strict/partial read equivalence, bar invariants, validation, and file-derived catalog/strict-read equivalence after rebuild. The strengthened campaign found a CCXT calendar-rounding defect for `1w` and `1M`; those timeframes are explicitly untrusted pending remediation and requalification.

### Gate

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | Gate spot OHLCV | Spot | Time bars | `1h` | `BTC/USDT` |
| CCXT | Gate perpetual OHLCV | USDT-settled linear perpetual | Time bars | `1h` | `BTC/USDT` |

Gate spot and perpetual `1h` passed the earlier qualification gate over recent, seven-day, and one-year ranges, including canonical sync, strict reads, idempotent synchronization, partial reads, and catalog validation/rebuild.

### WOO

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | WOO spot OHLCV | Spot | Time bars | `1h` | `BTC/USDT` |
| CCXT | WOO perpetual OHLCV | USDT-settled linear perpetual | Time bars | `1h` | `BTC/USDT` |

WOO spot and perpetual `1h` passed the earlier qualification gate over recent, seven-day, and one-year ranges. Later forensics found duplicate timestamps in CCXT automatic pagination for selected long daily histories while the official endpoint remained unique. WOO therefore requires strengthened requalification and adapter review before promotion.

### Bitrue

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | Bitrue spot OHLCV | Spot | Time bars | `1h` | `BTC/USDT` |

Bitrue spot `1h` passed the earlier qualification gate. No perpetual market was selected, so this claim does not cover Bitrue perpetuals. Later forensics confirmed a native non-UTC daily boundary and an advertised futures `1h` endpoint that returns an empty response; these combinations require separate adapter or provider-capability decisions and remain outside the strengthened trust set.

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

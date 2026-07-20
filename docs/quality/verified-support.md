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

## Currently verified
The currently listed matrix was requalified on 2026-07-19 using the built `xret-data` 0.2.0 distribution in a fresh external uv environment after the readable metadata-first storage change.

### Binance

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | Binance USD-M klines | USDT-settled linear perpetual | Time bars | `1h` | `BTC/USDT` |

Binance USD-M perpetual `1h` qualification covers complete four-year synchronization, incremental extension, exact and concurrent canonical-data no-op synchronization, strict/partial read equivalence, bar invariants, validation, and file-derived catalog/strict-read equivalence after rebuild. Binance spot combinations remain unlisted pending re-verification after the storage-contract changes.

### Bybit

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | Bybit spot kline | Spot | Time bars | `1h` | `BTC/USDT` |
| CCXT | Bybit derivatives kline | USDT-settled linear perpetual | Time bars | `1h` | `BTC/USDT` |

Bybit spot and perpetual `1h` qualification covers complete four-year synchronization, incremental extension, exact and concurrent canonical-data no-op synchronization, strict/partial read equivalence, bar invariants, validation, and file-derived catalog/strict-read equivalence after rebuild. Bybit `1m` remains unlisted pending equivalent re-verification.

### OKX

| Provider | Endpoint family | Market family | Bar type | Timeframes | Representative symbols |
|---|---|---|---|---|---|
| CCXT | OKX market candles | Spot | Time bars | `1h` | `BTC/USDT` |

OKX spot `1h` qualification covers complete four-year synchronization, incremental extension, exact and concurrent canonical-data no-op synchronization, strict/partial read equivalence, bar invariants, validation, and file-derived catalog/strict-read equivalence after rebuild. OKX perpetual combinations remain unlisted pending equivalent re-verification.

A rebuild restores Parquet-provable datasets, files, and available coverage; it intentionally does not restore unavailable observations, ingestion runs, warnings, or quality events.

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
